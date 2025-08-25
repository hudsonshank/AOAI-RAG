import json
import logging
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import os
import datetime
from typing import Dict, Any, List
import hashlib
import random
from pptx import Presentation  # Add this import for PowerPoint extraction
from io import BytesIO
import openpyxl
from urllib.parse import quote

# Create the function app instance
app = func.FunctionApp()

#@app.timer_trigger(schedule="0 0 */6 * * *", arg_name="timer")
#def scan_onedrive_documents(timer: func.TimerRequest) -> None:
  #  """
 #   Cron-expression function that scans OneDrive/SharePoint for new documents
 #   Runs every 6 hours to find and process new documents
 #   """
 #   logging.info('OneDrive document scan started')
    
  #  try:
   #     processor = DocumentProcessor()
   #     results = processor.process_all_documents()
        
    #    logging.info(f'Document scan completed: {results}')
        
    #except Exception as e:
    #    logging.error(f'Error during document scan: {str(e)}')
   #     raise

@app.route(route="ProcessSingleDocument", methods=["POST"])
def process_single_document(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP endpoint for processing documents with optional multiple folder targeting or single file processing

    Expected JSON body:
    For folder processing:
    {
        "site_name": "Clients",
        "folder_paths": [
            "Shared Documents/Folder1/SubfolderA",
            "Shared Documents/Folder2/SubfolderB",
            ...
        ]
    }
    
    For single file processing:
    {
        "site_name": "Clients",
        "folder_path": "Shared Documents/Folder1",
        "file_name": "document.pdf"
    }
    """
    logging.info('Manual document processing triggered')
    try:
        req_body = req.get_json()
        if not req_body or not req_body.get('site_name'):
            return func.HttpResponse(
                json.dumps({"error": "Provide site_name in request body"}),
                status_code=400,
                mimetype="application/json"
            )
        site_name = req_body['site_name']
        file_name = req_body.get('file_name')
        
        # Check if processing a single file
        if file_name:
            folder_path = req_body.get('folder_path')
            processor = DocumentProcessor()
            result = processor.process_single_file(site_name, folder_path, file_name)
            return func.HttpResponse(
                json.dumps(result, indent=2),
                status_code=200,
                mimetype="application/json"
            )
        
        # Support both single and multiple folder paths
        folder_paths = req_body.get('folder_paths')
        if not folder_paths:
            folder_path = req_body.get('folder_path')
            folder_paths = [folder_path] if folder_path else [None]

        processor = DocumentProcessor()
        results = []
        for folder_path in folder_paths:
            if folder_path:
                result = processor.process_site_documents(site_name, folder_path)
            else:
                result = processor.process_site_documents(site_name)
            results.append(result)

        return func.HttpResponse(
            json.dumps(results, indent=2),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f'Error processing document: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

class DocumentProcessor:
    def __init__(self):
        """Initialize Azure services and Graph API connection"""
        try:
            # Load environment variables
            self.storage_connection = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            self.key_vault_url = os.environ.get('AZURE_KEY_VAULT_URL')
            self.doc_intelligence_endpoint = os.environ.get('DOCUMENT_INTELLIGENCE_ENDPOINT')
            self.doc_intelligence_key = os.environ.get('DOCUMENT_INTELLIGENCE_KEY')
            self.pii_function_url = os.environ.get('PII_FUNCTION_URL')
            self.pii_function_key = os.environ.get('PII_FUNCTION_KEY')
            
            # *** Cost Control Settings ***
            self.test_mode = os.environ.get('TEST_MODE', 'false').lower() == 'true'
            self.max_documents_per_run = int(os.environ.get('MAX_DOCUMENTS_PER_RUN', '50'))
            self.max_file_size_mb = int(os.environ.get('MAX_FILE_SIZE_MB', '100'))
            self.skip_doc_intelligence_for_large_files = True
            
            # Initialize Azure clients
            self.storage_client = BlobServiceClient.from_connection_string(self.storage_connection)
            self.key_vault_client = SecretClient(vault_url=self.key_vault_url, credential=DefaultAzureCredential())
            self.doc_intelligence_client = DocumentAnalysisClient(
                endpoint=self.doc_intelligence_endpoint,
                credential=AzureKeyCredential(self.doc_intelligence_key)
            )
            
            # Get Graph API token
            self.graph_token = self._get_graph_token()
            
            # Define supported file types with size preferences
            self.supported_extensions = {'.pdf', '.docx', '.doc', '.xlsx', '.xls', '.pptx', '.ppt', '.txt'}
            self.lightweight_extensions = {'.txt', '.docx', '.doc'}  # Process these first
            
            # SharePoint sites to scan
            self.sharepoint_sites = ["APM Board", "Clients", "Autobahn Overdrive Artificial Intelligence", "PM", "Operations", "The Glovebox", "Communication site"]
            
            # Enhanced folder filtering
            self.skip_folders = [
                "template emails", "archive [do not use]", "email templates", 
                "6. templates", "/templates/", "ser00", "health nucleus",
                "recycle bin", "forms/templates", "_private", "workflow history"
            ]
            
            # Processing statistics
            self.processing_stats = {
                "documents_processed_this_run": 0,
                "total_cost_estimate": 0.0,
                "processing_start_time": datetime.datetime.utcnow()
            }
            
            logging.info(f'DocumentProcessor initialized - Test Mode: {self.test_mode}, Max Docs: {self.max_documents_per_run}')
            
        except Exception as e:
            logging.error(f'Failed to initialize DocumentProcessor: {str(e)}')
            raise
    
    def _get_graph_token(self) -> str:
        """Get Microsoft Graph API access token"""
        try:
            client_secret = self.key_vault_client.get_secret("Jennifur-Client-Secret").value
            
            token_data = {
                'client_id': '8bb9c943-15df-4b66-b52f-4c40debdee88',  # Your Jennifur-AI-Assistant app ID
                'client_secret': client_secret,
                'scope': 'https://graph.microsoft.com/.default',
                'grant_type': 'client_credentials'
            }
            
            response = requests.post(
                'https://login.microsoftonline.com/95be67cb-cbae-432a-b9ca-d3befaae2e0e/oauth2/v2.0/token',
                data=token_data
            )
            response.raise_for_status()
            
            return response.json()['access_token']
            
        except Exception as e:
            logging.error(f'Failed to get Graph API token: {str(e)}')
            raise
    
    def _get_checkpoint_blob_name(self, site_name: str, folder_path: str = None) -> str:
        """Generate checkpoint blob name for a site and folder path"""
        import re
        
        # Sanitize site name
        safe_site_name = re.sub(r'[^a-zA-Z0-9-_]', '_', site_name)
        
        # Sanitize folder path
        if folder_path:
            safe_folder_path = re.sub(r'[^a-zA-Z0-9-_/]', '_', folder_path)
            safe_folder_path = safe_folder_path.strip('/').replace('/', '_')
        else:
            safe_folder_path = 'root'
        
        return f"checkpoints/{safe_site_name}/{safe_folder_path}.json"
    
    def _load_checkpoint(self, site_name: str, folder_path: str = None) -> Dict[str, Any]:
        """Load checkpoint from Azure Blob Storage"""
        try:
            blob_name = self._get_checkpoint_blob_name(site_name, folder_path)
            blob_client = self.storage_client.get_blob_client(
                container="processed-documents", 
                blob=blob_name
            )
            
            if blob_client.exists():
                checkpoint_data = blob_client.download_blob().readall()
                checkpoint = json.loads(checkpoint_data.decode('utf-8'))
                logging.info(f'📍 Loaded checkpoint for {site_name}/{folder_path or "root"}')
                return checkpoint
            else:
                logging.info(f'📍 No checkpoint found for {site_name}/{folder_path or "root"}')
                return {}
                
        except Exception as e:
            logging.warning(f'Failed to load checkpoint for {site_name}/{folder_path or "root"}: {str(e)}')
            return {}
    
    def _save_checkpoint(self, site_name: str, folder_path: str = None, next_link: str = None) -> None:
        """Save checkpoint to Azure Blob Storage"""
        try:
            checkpoint = {
                "site_name": site_name,
                "folder_path": folder_path or "/",
                "last_processed_url": next_link,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            
            blob_name = self._get_checkpoint_blob_name(site_name, folder_path)
            blob_client = self.storage_client.get_blob_client(
                container="processed-documents", 
                blob=blob_name
            )
            
            checkpoint_json = json.dumps(checkpoint, indent=2)
            blob_client.upload_blob(
                checkpoint_json, 
                overwrite=True, 
                content_type='application/json'
            )
            
            logging.info(f'💾 Saved checkpoint for {site_name}/{folder_path or "root"}')
            
        except Exception as e:
            logging.error(f'Failed to save checkpoint for {site_name}/{folder_path or "root"}: {str(e)}')
    
    def process_all_documents(self) -> Dict[str, Any]:
        """Process documents from all SharePoint sites"""
        
        results = {
            "scan_timestamp": datetime.datetime.utcnow().isoformat(),
            "sites_processed": 0,
            "total_documents_found": 0,
            "documents_processed": 0,
            "documents_quarantined": 0,
            "documents_flagged": 0,
            "documents_skipped": 0,
            "errors": []
        }
        
        for site_name in self.sharepoint_sites:
            try:
                logging.info(f'Processing site: {site_name}')
                site_results = self.process_site_documents(site_name)
                
                results["sites_processed"] += 1
                results["total_documents_found"] += site_results["documents_found"]
                results["documents_processed"] += site_results["documents_processed"]
                results["documents_quarantined"] += site_results["documents_quarantined"]
                results["documents_flagged"] += site_results["documents_flagged"]
                results["documents_skipped"] += site_results["documents_skipped"]
                
            except Exception as e:
                error_msg = f'Error processing site {site_name}: {str(e)}'
                logging.error(f'{error_msg}')
                results["errors"].append(error_msg)
        
        return results
    
    def process_single_file(self, site_name: str, folder_path: str, file_name: str) -> Dict[str, Any]:
        """Process a single specific file from SharePoint
        
        Args:
            site_name: Name of the SharePoint site
            folder_path: Path to the folder containing the file
            file_name: Name of the specific file to process
        """
        result = {
            "site_name": site_name,
            "folder_path": folder_path,
            "file_name": file_name,
            "status": "failed",
            "action": "unknown",
            "reason": None,
            "cost_estimate": 0.0
        }
        
        try:
            # Get site ID
            site_id = self._get_site_id(site_name)
            if not site_id:
                result["reason"] = f"Site not found: {site_name}"
                return result
            
            # Find the specific file
            file_doc = self._get_specific_file(site_id, folder_path, file_name)
            if not file_doc:
                result["reason"] = f"File not found: {file_name} in {folder_path}"
                return result
            
            logging.info(f"Found specific file: {file_name}")
            
            # Process the single document
            process_result = self._process_single_document_with_cost_control(file_doc)
            
            # Update result with processing outcome
            result.update(process_result)
            result["status"] = "completed"
            
            return result
            
        except Exception as e:
            logging.error(f'Error processing single file {file_name}: {str(e)}')
            result["reason"] = str(e)
            return result
    
    def process_site_documents(self, site_name: str, folder_path: str = None) -> Dict[str, Any]:
        """Process documents from a specific SharePoint site with comprehensive error tracking
        
        Args:
            site_name: Name of the SharePoint site
            folder_path: Optional folder path to target (e.g., "Shared Documents/Autobahn Tools/1-3-1 Problem Solving")
        """
        
        site_results = {
            "site_name": site_name,
            "test_mode": self.test_mode,
            "documents_found": 0,
            "documents_processed": 0,
            "documents_quarantined": 0,
            "documents_flagged": 0,
            "documents_skipped": 0,
            "documents_failed": 0,
            "documents_skipped_size_limit": 0,
            "documents_skipped_extraction_failed": 0,
            "folders_skipped": 0,
            "processing_errors": [],
            "file_type_summary": {},
            "cost_estimate": 0.0,
            "total_processing_time_seconds": 0
        }
        
        processing_start = datetime.datetime.utcnow()
        
        try:
            # Get site ID
            site_id = self._get_site_id(site_name)
            if not site_id:
                logging.warning(f'⚠️ Site not found: {site_name}')
                return site_results
            
            # Load checkpoint for resuming from last position
            checkpoint = self._load_checkpoint(site_name, folder_path)
            
            # Get documents from site
            documents = self._get_site_documents(site_id, folder_path, site_name, checkpoint)
            site_results["documents_found"] = len(documents)
            logging.info(f"[BATCH] Found {len(documents)} documents in SharePoint for site '{site_name}' and folder '{folder_path or '/'}'")
            
            # Add folder path to results for tracking
            site_results["folder_path"] = folder_path or "/"

            # Prioritize documents for RAG ingestion
            documents_to_process = self._prioritize_documents_for_testing(documents)
            logging.info(f"[BATCH] Prioritized {len(documents_to_process)} documents for processing (max per run: {self.max_documents_per_run})")

            processed_count = 0
            # Process each document with cost tracking
            for doc in documents_to_process:
                # Check if we've hit our processing limit
                if self.processing_stats["documents_processed_this_run"] >= self.max_documents_per_run:
                    logging.info(f'🛑 Hit processing limit ({self.max_documents_per_run}), stopping')
                    break
                
                try:
                    result = self._process_single_document_with_cost_control(doc)
                    
                    # Update counters
                    action = result.get("action", "unknown")
                    extension = result.get("extension", "unknown")
                    cost = result.get("cost_estimate", 0.0)
                    
                    # Update processing stats
                    if action in ["processed", "quarantined", "flagged"]:
                        self.processing_stats["documents_processed_this_run"] += 1
                        self.processing_stats["total_cost_estimate"] += cost
                        site_results["cost_estimate"] += cost
                    
                    # Count by action
                    if action == "processed":
                        site_results["documents_processed"] += 1
                    elif action == "quarantined":
                        site_results["documents_quarantined"] += 1
                    elif action == "flagged":
                        site_results["documents_flagged"] += 1
                    elif action == "skipped":
                        site_results["documents_skipped"] += 1
                    elif action == "skipped_size_limit":
                        site_results["documents_skipped_size_limit"] += 1
                    elif action == "skipped_extraction_failed":
                        site_results["documents_skipped_extraction_failed"] += 1
                    else:
                        site_results["documents_failed"] += 1
                        site_results["processing_errors"].append({
                            "file": result.get("path", "unknown"),
                            "reason": result.get("reason", "unknown"),
                            "extension": extension
                        })
                    
                    # Count by file type
                    site_results["file_type_summary"][extension] = site_results["file_type_summary"].get(extension, 0) + 1
                    
                except Exception as e:
                    logging.error(f'❌ Critical error processing document {doc.get("name", "unknown")}: {str(e)}')
                    site_results["documents_failed"] += 1
                    site_results["processing_errors"].append({
                        "file": doc.get("path", doc.get("name", "unknown")),
                        "reason": f"critical_error: {str(e)}",
                        "extension": doc.get("extension", "unknown")
                    })
            
            # Calculate total processing time
            processing_end = datetime.datetime.utcnow()
            site_results["total_processing_time_seconds"] = round((processing_end - processing_start).total_seconds(), 2)
            
            # Enhanced logging with cost information
            logging.info(f'Site {site_name} processing complete:')
            logging.info(f'   Processed: {site_results["documents_processed"]}')
            logging.info(f'   Quarantined: {site_results["documents_quarantined"]}')
            logging.info(f'   Flagged: {site_results["documents_flagged"]}')
            logging.info(f'   Skipped (already processed): {site_results["documents_skipped"]}')
            logging.info(f'   Skipped (size limit): {site_results["documents_skipped_size_limit"]}')
            logging.info(f'   Skipped (extraction failed): {site_results["documents_skipped_extraction_failed"]}')
            logging.info(f'   Failed: {site_results["documents_failed"]}')
            logging.info(f'   Estimated cost: ${site_results["cost_estimate"]:.4f}')
            logging.info(f'   Total time: {site_results["total_processing_time_seconds"]}s')
            logging.info(f"[BATCH] Actually processed {processed_count} documents in this run.")
            
            return site_results
            
        except Exception as e:
            processing_end = datetime.datetime.utcnow()
            site_results["total_processing_time_seconds"] = round((processing_end - processing_start).total_seconds(), 2)
            error_msg = f'Error processing site {site_name}: {str(e)}'
            logging.error(error_msg)
            site_results["processing_errors"].append({
                "site": site_name,
                "reason": f"site_processing_error: {str(e)}",
                "extension": "N/A"
            })
            return site_results
    
    def _get_site_id(self, site_name: str) -> str:
        """Get SharePoint site ID by name"""
        try:
            headers = {'Authorization': f'Bearer {self.graph_token}'}
            
            # Search for site
            response = requests.get(
                f'https://graph.microsoft.com/v1.0/sites?search={site_name}',
                headers=headers
            )
            response.raise_for_status()
            
            sites = response.json().get('value', [])
            if sites:
                return sites[0]['id']
            
            return None
            
        except Exception as e:
            logging.error(f'Error getting site ID for {site_name}: {str(e)}')
            return None
    
    def _get_site_documents(self, site_id: str, folder_path: str = None, site_name: str = None, checkpoint: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Get all documents from a SharePoint site, optionally targeting a specific folder with checkpoint support"""
        try:
            headers = {'Authorization': f'Bearer {self.graph_token}'}
            documents = []
            
            # Get document libraries in the site
            drives_response = requests.get(
                f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives',
                headers=headers
            )
            drives_response.raise_for_status()
            
            drives_data = drives_response.json()
            # Log all drive names and IDs for debugging
            logging.info(f"[DEBUG] Drives for site {site_name} (site_id={site_id}):")
            for drive in drives_data['value']:
                logging.info(f"[DEBUG] Drive name: '{drive.get('name')}', ID: {drive.get('id')}")
            
            for drive in drives_data['value']:
                drive_id = drive['id']
                # If targeting a specific folder, get documents from that folder only
                if folder_path:
                    # Remove drive name from path if it's included (e.g., "Documents/Folder" -> "Folder")
                    clean_folder_path = folder_path
                    for current_drive in drives_data['value']:
                        if folder_path.startswith(f"{current_drive['name']}/"):
                            clean_folder_path = folder_path[len(f"{current_drive['name']}/"):]
                            break
                    
                    logging.info(f"�� Targeting specific folder: {clean_folder_path}")
                    documents = self._get_drive_documents(drive_id, clean_folder_path, checkpoint)
                else:
                    # Start from root if no folder specified
                    documents.extend(self._get_drive_documents(drive_id, None, checkpoint))
            
            return documents
            
        except Exception as e:
            logging.error(f'Error getting documents from site {site_id}: {str(e)}')
            return []
    
    def _get_drive_documents(self, drive_id: str, folder_path: str = None, site_name: str = None, checkpoint: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Get documents from a specific drive with sequential pagination and checkpoint support"""
        try:
            headers = {'Authorization': f'Bearer {self.graph_token}'}
            all_documents = []
            api_calls_made = 0
            max_api_calls = 500  # Increased from 100 to allow deeper scanning
            
            # Determine starting URL based on folder_path and checkpoint
            if folder_path:
                # If folder_path is specified, start from that specific folder
                start_url = self._build_folder_url(drive_id, folder_path)
                logging.info(f'📁 Targeting specific folder: {folder_path}')
            else:
                # Start from root if no folder specified
                start_url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children?$top=100'
                logging.info(f'📁 Starting from root folder')
            
            # Resume from checkpoint if available
            current_url = checkpoint.get('last_processed_url') if checkpoint else start_url
            if checkpoint and checkpoint.get('last_processed_url'):
                logging.info(f'📍 Resuming from checkpoint URL')
                current_url = checkpoint['last_processed_url']
            else:
                current_url = start_url
            
            # Sequential pagination through documents
            while current_url and api_calls_made < max_api_calls:
                try:
                    logging.info(f'📡 Making Graph API call #{api_calls_made + 1} - Found {len(all_documents)} documents so far')
                    response = requests.get(current_url, headers=headers)
                    response.raise_for_status()
                    api_calls_made += 1
                    
                    items = response.json()
                    items_list = items.get('value', [])
                    
                    # Process items sequentially (no randomization)
                    for item in items_list:
                        item_name = item['name']
                        item_path = self._build_item_path(folder_path or '/', item_name)
                        
                        # Check if it's a file (skip folders for now - focusing on targeted folder processing)
                        if 'file' in item:
                            file_ext = os.path.splitext(item_name)[1].lower()
                            
                            # Check if supported file type
                            if file_ext in self.supported_extensions:
                                all_documents.append({
                                    'id': item['id'],
                                    'name': item_name,
                                    'path': item_path,
                                    'download_url': item.get('@microsoft.graph.downloadUrl', ''),
                                    'last_modified': item.get('lastModifiedDateTime', ''),
                                    'size': item.get('size', 0),
                                    'folder_path': folder_path or '/',
                                    'extension': file_ext
                                })
                                logging.info(f'Found document: {item_path}')
                                
                                # Check if we've hit our discovery limit (higher than processing limit)
                                discovery_limit = self.max_documents_per_run * 10  # Find 10x more documents for better selection
                                if len(all_documents) >= discovery_limit:
                                    logging.info(f'🛑 Hit discovery limit ({discovery_limit}), will prioritize and select best documents')
                                    # Save checkpoint with next URL
                                    next_url = items.get('@odata.nextLink')
                                    if next_url and site_name:
                                        self._save_checkpoint(site_name, folder_path, next_url)
                                    return all_documents
                    
                    # Get next page URL
                    next_url = items.get('@odata.nextLink')
                    
                    # Save checkpoint after each successful page
                    if site_name:
                        self._save_checkpoint(site_name, folder_path, next_url)
                    
                    current_url = next_url
                    
                except requests.exceptions.RequestException as e:
                    logging.error(f'Error during pagination: {str(e)}')
                    api_calls_made += 1  # Count failed calls too
                    break
            
            # Clear checkpoint if we've completed all pages
            if not current_url and site_name:
                self._save_checkpoint(site_name, folder_path, None)
                logging.info(f'✅ Completed processing all documents in folder')
            
            # Log final statistics
            logging.info(f'📊 Sequential scan completed:')
            logging.info(f'   Documents found: {len(all_documents)}')
            logging.info(f'   API calls made: {api_calls_made}')
            logging.info(f'   📋 Documents processed sequentially (no randomization)')
            
            if api_calls_made >= max_api_calls:
                logging.warning(f'⚠️ Stopped due to API call limit. Will resume from checkpoint on next run.')
            
            return all_documents
            
        except Exception as e:
            logging.error(f'Error getting documents from drive {drive_id}: {str(e)}')
            return []
    
    def _get_specific_file(self, site_id: str, folder_path: str, file_name: str) -> Dict[str, Any]:
        """Get a specific file from SharePoint
        
        Args:
            site_id: SharePoint site ID
            folder_path: Path to the folder containing the file
            file_name: Name of the specific file
            
        Returns:
            Document dictionary if found, None otherwise
        """
        try:
            headers = {'Authorization': f'Bearer {self.graph_token}'}
            
            # Get document libraries in the site
            drives_response = requests.get(
                f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives',
                headers=headers
            )
            drives_response.raise_for_status()
            drives_data = drives_response.json()
            
            for drive in drives_data['value']:
                drive_id = drive['id']
                
                # Clean up folder path
                clean_folder_path = folder_path
                for current_drive in drives_data['value']:
                    if folder_path.startswith(f"{current_drive['name']}/"):
                        clean_folder_path = folder_path[len(f"{current_drive['name']}/"):]
                        break
                
                # Build URL for the specific file
                if clean_folder_path:
                    folder_url = self._build_folder_url(drive_id, clean_folder_path)
                    file_url = f"{folder_url}/children?$filter=name eq '{file_name}'"
                else:
                    file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children?$filter=name eq '{file_name}'"
                
                # Search for the file
                response = requests.get(file_url, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                if data.get('value'):
                    # File found, return the first match
                    file_item = data['value'][0]
                    return {
                        'id': file_item['id'],
                        'name': file_item['name'],
                        'path': f"{folder_path}/{file_name}" if folder_path else file_name,
                        'size': file_item['size'],
                        'download_url': file_item.get('@microsoft.graph.downloadUrl', ''),
                        'drive_id': drive_id,
                        'last_modified': file_item.get('lastModifiedDateTime', ''),
                        'web_url': file_item.get('webUrl', '')
                    }
            
            return None
            
        except Exception as e:
            logging.error(f'Error finding specific file {file_name} in {folder_path}: {str(e)}')
            return None
    
    def _build_folder_url(self, drive_id: str, folder_path: str) -> str:
        """Build Graph API URL for a specific folder path"""
        # Clean up folder path
        clean_path = folder_path.strip('/')
        # Properly encode each segment
        encoded_path = '%2F'.join([quote(seg, safe='') for seg in clean_path.split('/')])
        return f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{encoded_path}:/children?$top=100'
    
    def _build_item_path(self, folder_path: str, item_name: str) -> str:
        """Build full item path from folder path and item name"""
        if folder_path == '/':
            return f'/{item_name}'
        else:
            return f"{folder_path.rstrip('/')}/{item_name}"
    
    def _prioritize_documents_for_testing(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Smart document prioritization with randomization to avoid processing same documents"""
        
        # *** NEW: Filter out already processed documents first ***
        unprocessed_documents = []
        for doc in documents:
            if not self._is_document_processed(doc['id']):
                unprocessed_documents.append(doc)
        
        logging.info(f'📊 Found {len(unprocessed_documents)} unprocessed documents out of {len(documents)} total')
        
        # If we have enough unprocessed documents, use those
        if len(unprocessed_documents) >= self.max_documents_per_run:
            documents_to_prioritize = unprocessed_documents
        else:
            # Use all documents if we don't have enough unprocessed ones
            documents_to_prioritize = documents
            logging.info(f'⚠️ Using all documents including processed ones (need {self.max_documents_per_run}, found {len(unprocessed_documents)} unprocessed)')
        
        def document_priority(doc):
            extension = doc.get('extension', '').lower()
            size = doc.get('size', 0)
            last_modified = doc.get('last_modified', '')
            
            # Priority score (lower = higher priority)
            priority = 0
            
            # *** NEW: Add randomization to priority ***
            priority += random.randint(0, 2)  # Add random factor to mix things up
            
            # Prioritize lightweight file types
            if extension in self.lightweight_extensions:
                priority += 1
            elif extension in ['.pdf']:
                priority += 3
            elif extension in ['.xlsx', '.xls']:
                priority += 4
            else:
                priority += 2
            
            # Prioritize smaller files
            if size < 1024 * 1024:  # < 1MB
                priority += 1
            elif size > 10 * 1024 * 1024:  # > 10MB
                priority += 5
            
            # Prioritize recently modified (parse date if available)
            try:
                if last_modified:
                    mod_date = datetime.datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                    days_old = (datetime.datetime.utcnow().replace(tzinfo=mod_date.tzinfo) - mod_date).days
                    if days_old < 30:  # Modified in last 30 days
                        priority += 1
            except:
                pass
            
            # *** NEW: Boost priority for unprocessed documents ***
            if not self._is_document_processed(doc['id']):
                priority -= 5  # Higher priority for unprocessed docs
            
            return priority
        
        # Sort by priority and take the first max_documents_per_run
        sorted_docs = sorted(documents_to_prioritize, key=document_priority)
        selected_docs = sorted_docs[:self.max_documents_per_run]
        
        # *** NEW: Final randomization within selected documents ***
        random.shuffle(selected_docs)
        
        logging.info(f'🎲 Selected {len(selected_docs)} documents with randomization and priority')
        
        return selected_docs

    def _process_single_document_with_cost_control(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single document with enhanced cost control and size limits"""
        
        doc_id = doc['id']
        doc_name = doc['name']
        doc_path = doc.get('path', doc_name)
        doc_extension = doc.get('extension', 'unknown')
        doc_size = doc.get('size', 0)
        
        processing_start = datetime.datetime.utcnow()
        estimated_cost = 0.0
        
        try:
            # Check if already processed
            if self._is_document_processed(doc_id):
                logging.info(f'Document {doc_path} already processed, skipping')
                return {
                    "action": "skipped", 
                    "reason": "already_processed", 
                    "path": doc_path,
                    "extension": doc_extension,
                    "cost_estimate": 0.0
                }
            
            # Size-based processing limits
            max_size_bytes = self.max_file_size_mb * 1024 * 1024
            if doc_size > max_size_bytes:
                if self.skip_doc_intelligence_for_large_files:
                    logging.info(f'📏 Large file {doc_path} ({doc_size:,} bytes), using fallback processing')
                    return self._process_large_file_with_fallback(doc, doc_size)
                else:
                    logging.info(f'📏 File {doc_path} exceeds size limit ({doc_size:,} bytes), skipping')
                    return {
                        "action": "skipped_size_limit",
                        "reason": f"file_too_large_{doc_size}_bytes",
                        "path": doc_path,
                        "extension": doc_extension,
                        "cost_estimate": 0.0
                    }
            
            logging.info(f'Processing document: {doc_path} ({doc_extension}, {doc_size:,} bytes)')
            
            # Download document with error handling
            try:
                doc_content = self._download_document(doc['download_url'])
                download_size = len(doc_content)
                logging.info(f'Downloaded {download_size:,} bytes for {doc_name}')
                
            except Exception as e:
                logging.error(f'Failed to download {doc_path}: {str(e)}')
                return {
                    "action": "error", 
                    "reason": f"download_failed: {str(e)}", 
                    "path": doc_path,
                    "extension": doc_extension,
                    "cost_estimate": 0.0
                }
            
            # Extract text with cost estimation
            try:
                extracted_text, text_extraction_cost, extraction_success = self._extract_text_with_cost_tracking(doc_content, doc_name)
                estimated_cost += text_extraction_cost
                
                # Add folder path context to extracted text
                folder_context = f"Document Location: {doc_path}\n"
                folder_context += f"Folder: {doc.get('folder_path', '/')}\n"
                folder_context += f"File Extension: {doc_extension}\n"
                folder_context += f"File Size: {download_size:,} bytes\n\n"
                extracted_text = folder_context + extracted_text
                
            except Exception as e:
                logging.error(f'Text extraction completely failed for {doc_path}: {str(e)}')
                extracted_text = self._create_fallback_text_content(doc_name, f"Complete extraction failure: {str(e)}")
                extraction_success = False
            
            # Handle failed extractions - skip instead of processing
            if not extraction_success:
                logging.info(f'⏭️ Skipping document {doc_path} - Document Intelligence extraction failed')
                result_base = {
                    "path": doc_path,
                    "extension": doc_extension,
                    "processing_duration_seconds": round((datetime.datetime.utcnow() - processing_start).total_seconds(), 2),
                    "file_size_bytes": download_size if 'download_size' in locals() else 0,
                    "cost_estimate": round(estimated_cost, 4)
                }
                return {**result_base, "action": "skipped_extraction_failed", "reason": "document_intelligence_failed"}
            
            # Run PII detection only for successfully extracted text
            try:
                pii_results = self._run_pii_detection(extracted_text, doc_name)
                estimated_cost += 0.001  # Minimal cost for PII detection
                
            except Exception as e:
                logging.error(f'PII detection failed for {doc_path}: {str(e)}')
                pii_results = {
                    "overall_severity": "unknown",
                    "recommended_action": "flag_for_review",
                    "error": f"PII detection failed: {str(e)}"
                }
            
            # Determine action and store document
            action = pii_results.get('recommended_action', 'log_and_allow')
            processing_duration = (datetime.datetime.utcnow() - processing_start).total_seconds()
            
            result_base = {
                "path": doc_path,
                "extension": doc_extension,
                "processing_duration_seconds": round(processing_duration, 2),
                "file_size_bytes": download_size if 'download_size' in locals() else 0,
                "cost_estimate": round(estimated_cost, 4)
            }
            
            # Store document based on PII results
            if action == 'quarantine_immediately':
                try:
                    self._store_in_quarantine(doc_id, doc_name, extracted_text, pii_results, doc_path)
                    logging.info(f'Document {doc_path} quarantined due to PII')
                    return {**result_base, "action": "quarantined", "reason": "critical_pii_detected"}
                except Exception as e:
                    logging.error(f'Failed to quarantine {doc_path}: {str(e)}')
                    return {**result_base, "action": "error", "reason": f"quarantine_failed: {str(e)}"}
                    
            elif action == 'flag_for_review':
                try:
                    self._store_for_review(doc_id, doc_name, extracted_text, pii_results, doc_path)
                    logging.info(f'⚠️ Document {doc_path} flagged for review')
                    return {**result_base, "action": "flagged", "reason": "pii_review_needed"}
                except Exception as e:
                    logging.error(f'Failed to flag for review {doc_path}: {str(e)}')
                    return {**result_base, "action": "error", "reason": f"review_storage_failed: {str(e)}"}
                    
            else:
                try:
                    self._store_processed_document(doc_id, doc_name, extracted_text, doc, doc_path)
                    logging.info(f'✅ Document {doc_path} processed successfully (${estimated_cost:.4f})')
                    return {**result_base, "action": "processed", "reason": "clean_document"}
                except Exception as e:
                    logging.error(f'Failed to store processed document {doc_path}: {str(e)}')
                    return {**result_base, "action": "error", "reason": f"storage_failed: {str(e)}"}
            
        except Exception as e:
            processing_duration = (datetime.datetime.utcnow() - processing_start).total_seconds()
            logging.error(f'Unexpected error processing document {doc_path}: {str(e)}')
            return {
                "action": "error", 
                "reason": f"unexpected_error: {str(e)}", 
                "path": doc_path,
                "extension": doc_extension,
                "processing_duration_seconds": round(processing_duration, 2),
                "cost_estimate": round(estimated_cost, 4)
            }

    def _extract_text_with_cost_tracking(self, doc_content: bytes, filename: str) -> tuple[str, float, bool]:
        """Extract text and track estimated costs. Returns (text, cost, success_flag)"""
        
        # Validate document first
        if not self._validate_document_before_processing(doc_content, filename):
            return self._create_fallback_text_content(filename, "File validation failed"), 0.0, False
        
        file_ext = os.path.splitext(filename)[1].lower()
        file_size_mb = len(doc_content) / (1024 * 1024)
        
        # Estimated cost calculation (based on Azure Document Intelligence pricing)
        # Approximately $1.00 per 1,000 pages, estimate ~0.5MB per page
        estimated_pages = max(1, file_size_mb / 0.5)
        estimated_cost = estimated_pages * 0.001  # $0.001 per page
        
        try:
            logging.info(f'Analyzing document with Document Intelligence: {filename} (Est. cost: ${estimated_cost:.4f})')
            
            # Analyze document
            poller = self.doc_intelligence_client.begin_analyze_document(
                "prebuilt-document", 
                doc_content
            )
            result = poller.result()
            
            # Extract content based on file type
            if file_ext in ['.xlsx', '.xls']:
                extracted_text = self._extract_excel_content(result, filename)
            else:
                extracted_text = self._extract_standard_content(result, filename)
            
            return extracted_text, estimated_cost, True
            
        except Exception as e:
            error_message = str(e)
            
            # PowerPoint fallback: try extracting text locally if DI fails
            if file_ext in ['.pptx', '.ppt']:
                try:
                    prs = Presentation(BytesIO(doc_content))
                    pptx_text = f"PowerPoint Document: {filename}\n"
                    pptx_text += f"Processed with: python-pptx fallback (DI unavailable)\n"
                    pptx_text += f"Processing date: {datetime.datetime.utcnow().isoformat()}\n\n"
                    for i, slide in enumerate(prs.slides):
                        pptx_text += f"--- Slide {i+1} ---\n"
                        for shape in slide.shapes:
                            if hasattr(shape, "text"):
                                pptx_text += shape.text + "\n"
                        pptx_text += "\n"
                    if pptx_text.strip():
                        logging.info(f'Successfully extracted {len(pptx_text):,} characters from PowerPoint file {filename} using python-pptx fallback')
                        return pptx_text, 0.0, True
                except Exception as pptx_e:
                    logging.warning(f'python-pptx fallback failed for {filename}: {pptx_e}')
                    # Fall through to existing fallback logic
            # Excel fallback: try extracting text locally if DI fails and file is .xlsx
            if file_ext == '.xlsx':
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(BytesIO(doc_content), data_only=True)
                    excel_text = f"Excel Document: {filename}\n"
                    excel_text += f"Processed with: openpyxl fallback (DI unavailable)\n"
                    excel_text += f"Processing date: {datetime.datetime.utcnow().isoformat()}\n\n"
                    
                    for sheet in wb.worksheets:
                        sheet_has_content = False
                        sheet_content = f"--- Sheet: {sheet.title} ---\n"
                        
                        # Only process rows with actual data (skip empty rows)
                        for row in sheet.iter_rows(values_only=True):
                            # Filter out None/empty cells and join with pipe separators
                            row_data = [str(cell).strip() for cell in row if cell is not None and str(cell).strip()]
                            if row_data:  # Only add non-empty rows
                                if not sheet_has_content:
                                    excel_text += sheet_content
                                    sheet_has_content = True
                                excel_text += ' | '.join(row_data) + ' | \n'
                        
                        if sheet_has_content:
                            excel_text += '\n'
                    
                    if excel_text.strip():
                        logging.info(f'Successfully extracted {len(excel_text):,} characters from Excel file {filename} using openpyxl fallback (filtered empty cells)')
                        return excel_text, 0.0, True
                except Exception as xlsx_e:
                    logging.warning(f'openpyxl fallback failed for {filename}: {xlsx_e}')
                    # Fall through to existing fallback logic
            # Handle specific Document Intelligence errors
            if any(phrase in error_message for phrase in [
                "InvalidContent", "corrupted or format is unsupported",
                "UnsupportedMediaType", "InvalidRequest"
            ]):
                logging.warning(f'⚠️ Document Intelligence cannot process {filename}: {error_message}')
                if file_ext in ['.xlsx', '.xls']:
                    return self._create_excel_fallback_content(filename, error_message), 0.0, False
                else:
                    return self._create_fallback_text_content(filename, f"Document Intelligence error: {error_message}"), 0.0, False
            else:
                logging.error(f'Unexpected error extracting text from {filename}: {error_message}')
                if file_ext in ['.xlsx', '.xls']:
                    return self._create_excel_fallback_content(filename, f"Processing error: {error_message}"), 0.0, False
                else:
                    return self._create_fallback_text_content(filename, f"Processing error: {error_message}"), 0.0, False

    def _extract_excel_content(self, result, filename: str) -> str:
        """Extract content specifically formatted for Excel files with empty cell filtering"""
        
        extracted_text = f"Excel Document: {filename}\n"
        extracted_text += f"Processed with: Azure Document Intelligence (Excel Mode)\n"
        extracted_text += f"Processing date: {datetime.datetime.utcnow().isoformat()}\n\n"
        
        # Handle Excel tables with better formatting and empty cell filtering
        if result.tables:
            for table_idx, table in enumerate(result.tables):
                sheet_content = f"--- Sheet: {table_idx + 1} ---\n"
                
                # Group cells by row and filter out empty content
                rows_dict = {}
                for cell in table.cells:
                    cell_content = cell.content.strip()
                    if cell_content:  # Only process non-empty cells
                        if cell.row_index not in rows_dict:
                            rows_dict[cell.row_index] = {}
                        rows_dict[cell.row_index][cell.column_index] = cell_content
                
                # Only add sheet if it has content
                if rows_dict:
                    extracted_text += sheet_content
                    
                    # Process rows that have content
                    for row_idx in sorted(rows_dict.keys()):
                        row_cells = rows_dict[row_idx]
                        if row_cells:  # Only process non-empty rows
                            # Create row with only non-empty cells
                            row_values = []
                            for col_idx in sorted(row_cells.keys()):
                                row_values.append(row_cells[col_idx])
                            
                            if row_values:  # Only add if row has content
                                extracted_text += " | ".join(row_values) + " | \n"
                    
                    extracted_text += "\n"
        
        # Also extract any general text content
        text_content = ""
        for page in result.pages:
            for line in page.lines:
                line_content = line.content.strip()
                if line_content:  # Only add non-empty lines
                    text_content += line_content + "\n"
        
        if text_content.strip():
            extracted_text += "--- Additional Text Content ---\n"
            extracted_text += text_content + "\n"
        
        logging.info(f'Successfully extracted {len(extracted_text):,} characters from Excel file {filename} (filtered empty cells)')
        return extracted_text

    def _extract_standard_content(self, result, filename: str) -> str:
        """Extract content for standard documents (PDF, Word, etc.)"""
        
        extracted_text = f"Document: {filename}\n"
        extracted_text += f"Processed with: Azure Document Intelligence\n"
        extracted_text += f"Processing date: {datetime.datetime.utcnow().isoformat()}\n\n"
        
        # Add page content
        for page_idx, page in enumerate(result.pages):
            extracted_text += f"--- Page {page_idx + 1} ---\n"
            for line in page.lines:
                extracted_text += line.content + "\n"
            extracted_text += "\n"
        
        # Add tables if present
        if result.tables:
            extracted_text += "--- Tables ---\n"
            for table_idx, table in enumerate(result.tables):
                extracted_text += f"Table {table_idx + 1}:\n"
                for cell in table.cells:
                    extracted_text += f"Row {cell.row_index}, Col {cell.column_index}: {cell.content}\n"
                extracted_text += "\n"
        
        logging.info(f'Successfully extracted {len(extracted_text):,} characters from {filename}')
        return extracted_text

    def _create_excel_fallback_content(self, filename: str, error_reason: str) -> str:
        """Create fallback content specifically for Excel files"""
        
        fallback_text = f"Excel Document: {filename}\n"
        fallback_text += f"Processed with: Fallback (Document Intelligence unavailable for Excel)\n"
        fallback_text += f"Processing date: {datetime.datetime.utcnow().isoformat()}\n"
        fallback_text += f"Reason: {error_reason}\n\n"
        fallback_text += f"--- Document Information ---\n"
        fallback_text += f"Filename: {filename}\n"
        fallback_text += f"File Type: Microsoft Excel Spreadsheet\n"
        fallback_text += f"Note: This Excel file was detected but could not be fully processed. "
        fallback_text += f"The file may contain:\n"
        fallback_text += f"- Complex formulas or calculations\n"
        fallback_text += f"- Charts, graphs, or visual elements\n"
        fallback_text += f"- Multiple sheets with complex formatting\n"
        fallback_text += f"- Macros or VBA code\n"
        fallback_text += f"- Protected or encrypted content\n\n"
        fallback_text += f"For full Excel processing, consider using specialized tools or manual extraction.\n"
        
        return fallback_text

    def _create_fallback_text_content(self, filename: str, reason: str) -> str:
        """Create fallback text content when Document Intelligence fails"""
        
        fallback_text = f"Document: {filename}\n"
        fallback_text += f"Processed with: Fallback (Document Intelligence unavailable)\n"
        fallback_text += f"Processing date: {datetime.datetime.utcnow().isoformat()}\n"
        fallback_text += f"Reason: {reason}\n\n"
        fallback_text += f"--- Document Information ---\n"
        fallback_text += f"Filename: {filename}\n"
        fallback_text += f"Note: This document could not be fully processed. "
        fallback_text += f"The document exists and was detected, but text extraction was not possible.\n"
        fallback_text += f"File may contain images, complex formatting, or be in an unsupported format.\n"
        
        return fallback_text

    def _process_large_file_with_fallback(self, doc: Dict[str, Any], file_size: int) -> Dict[str, Any]:
        """Process large files without Document Intelligence to save costs"""
        
        doc_id = doc['id']
        doc_name = doc['name']
        doc_path = doc.get('path', doc_name)
        doc_extension = doc.get('extension', 'unknown')
        
        try:
            # Create metadata-only content for large files
            fallback_text = f"Large Document: {doc_name}\n"
            fallback_text += f"File Size: {file_size:,} bytes ({file_size/(1024*1024):.1f} MB)\n"
            fallback_text += f"Document Location: {doc_path}\n"
            fallback_text += f"Processing Method: Metadata-only (size limit bypass)\n"
            fallback_text += f"Processing Date: {datetime.datetime.utcnow().isoformat()}\n\n"
            fallback_text += f"Note: This large file was cataloged but not fully processed to control costs.\n"
            fallback_text += f"The document is available in SharePoint at the location above.\n"
            
            # Store with minimal processing
            self._store_processed_document(doc_id, doc_name, fallback_text, doc, doc_path)
            
            return {
                "action": "processed",
                "reason": "large_file_metadata_only",
                "path": doc_path,
                "extension": doc_extension,
                "file_size_bytes": file_size,
                "cost_estimate": 0.0
            }
            
        except Exception as e:
            return {
                "action": "error",
                "reason": f"large_file_fallback_failed: {str(e)}",
                "path": doc_path,
                "extension": doc_extension,
                "cost_estimate": 0.0
            }

    def _validate_document_before_processing(self, doc_content: bytes, filename: str) -> bool:
        """Validate document before sending to Document Intelligence"""
        
        # Check file size (Document Intelligence has limits)
        max_size = 500 * 1024 * 1024  # 500MB limit
        if len(doc_content) > max_size:
            logging.warning(f'File {filename} too large ({len(doc_content):,} bytes), skipping Document Intelligence')
            return False
        
        # Check for minimum file size
        if len(doc_content) < 100:
            logging.warning(f'File {filename} too small ({len(doc_content)} bytes), might be corrupted')
            return False
        
        # Check for empty files
        if len(doc_content) == 0:
            logging.warning(f'File {filename} is empty, skipping')
            return False
        
        return True

    def _run_pii_detection(self, content: str, filename: str) -> Dict[str, Any]:
        """Run PII detection on document content - TEMPORARILY DISABLED"""
        # PII detection is temporarily disabled for financial document processing
        logging.info(f'⚠️  PII detection DISABLED for {filename} - allowing all documents')
        return {
            "recommended_action": "log_and_allow", 
            "pii_detections": [], 
            "confidence_score": 0.0,
            "severity": "none",
            "status": "disabled",
            "message": "PII detection temporarily disabled for financial document processing"
        }
    
    def _store_in_quarantine(self, doc_id: str, filename: str, content: str, pii_results: Dict[str, Any], doc_path: str = None):
        """Store document in quarantine container"""
        try:
            quarantine_data = {
                "document_id": doc_id,
                "filename": filename,
                "document_path": doc_path or filename,
                "quarantine_timestamp": datetime.datetime.utcnow().isoformat(),
                "reason": "critical_pii_detected",
                "pii_results": pii_results,
                "content": content
            }
            
            blob_client = self.storage_client.get_blob_client(
                container="jennifur-quarantine",
                blob=f"{doc_id}.json"
            )
            
            blob_client.upload_blob(
                json.dumps(quarantine_data, indent=2),
                overwrite=True
            )
            
        except Exception as e:
            logging.error(f'Error storing document in quarantine: {str(e)}')
            raise

    def _store_for_review(self, doc_id: str, filename: str, content: str, pii_results: Dict[str, Any], doc_path: str = None):
        """Store document for manual review"""
        try:
            review_data = {
                "document_id": doc_id,
                "filename": filename,
                "document_path": doc_path or filename,
                "review_timestamp": datetime.datetime.utcnow().isoformat(),
                "status": "pending_review",
                "reason": "pii_review_required",
                "pii_results": pii_results,
                "content": content
            }
            
            blob_client = self.storage_client.get_blob_client(
                container="jennifur-excel-test",
                blob=f"review_{doc_id}.json"
            )
            
            blob_client.upload_blob(
                json.dumps(review_data, indent=2),
                overwrite=True
            )
            
        except Exception as e:
            logging.error(f'Error storing document for review: {str(e)}')
            raise

    def _store_processed_document(self, doc_id: str, filename: str, content: str, doc_metadata: Dict[str, Any], doc_path: str = None):
        """Store successfully processed document"""
        try:
            processed_data = {
                "document_id": doc_id,
                "filename": filename,
                "document_path": doc_path or filename,
                "processed_timestamp": datetime.datetime.utcnow().isoformat(),
                "status": "ready_for_rag",
                "content": content,
                "metadata": doc_metadata,
                "content_length": len(content)
            }
            
            blob_client = self.storage_client.get_blob_client(
                container="jennifur-excel-test",
                blob=f"{doc_id}.json"
            )
            
            blob_client.upload_blob(
                json.dumps(processed_data, indent=2),
                overwrite=True
            )
            
            logging.info(f'Document {filename} stored in processed container')
            
        except Exception as e:
            logging.error(f'Error storing processed document: {str(e)}')
            raise
    
    def _is_document_processed(self, doc_id: str) -> bool:
        """Check if document has already been processed"""
        try:
            blob_client = self.storage_client.get_blob_client(
                container="jennifur-excel-test",
                blob=f"{doc_id}.json"
            )
            return blob_client.exists()
        except Exception:
            return False
        
    def _download_document(self, download_url: str) -> bytes:
        """Download document content from OneDrive/SharePoint"""
        try:
            headers = {'Authorization': f'Bearer {self.graph_token}'}
            response = requests.get(download_url, headers=headers)
            response.raise_for_status()
            return response.content
            
        except Exception as e:
            logging.error(f'Error downloading document: {str(e)}')
            raise

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of current processing session"""
        
        total_time = (datetime.datetime.utcnow() - self.processing_stats["processing_start_time"]).total_seconds()
        
        return {
            "processing_session_summary": {
                "documents_processed": self.processing_stats["documents_processed_this_run"],
                "estimated_total_cost": round(self.processing_stats["total_cost_estimate"], 4),
                "session_duration_seconds": round(total_time, 2),
                "test_mode": self.test_mode,
                "max_documents_limit": self.max_documents_per_run,
                "max_file_size_mb": self.max_file_size_mb
            }
        }