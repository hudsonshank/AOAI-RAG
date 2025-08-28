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
from pptx import Presentation
from io import BytesIO
import openpyxl
from urllib.parse import quote
import re

# Import enhanced Excel processor with fallback
try:
    from src.utils.enhanced_excel_processor import EnhancedExcelProcessor
except ImportError:
    logging.warning("Enhanced Excel processor not available, using basic processing")
    EnhancedExcelProcessor = None

class ClientMetadataExtractor:
    """Extract client metadata from Autobahn Consultants SharePoint paths"""
    
    def __init__(self):
        # Pattern for client folders: /ClientName (PM-X)/
        self.client_pattern = re.compile(r'/([^/]+) \(PM-([A-Z])\)/')
        
        # PM name mappings
        self.pm_names = {
            'C': 'Caleb',
            'S': 'Sam', 
            'K': 'Katherine'
        }
        
        # Internal/tool folders (non-client specific)
        self.internal_folders = {
            'autobahn tools',
            'autobook and articles', 
            'pm & apm training materials',
            'archive [do not use]',
            'rock the recession',
            'to sort',
            'parking lot',
            'jennifur',
            'general resources',
            'the glovebox',
            'operations'
        }
        
        # Document category mappings based on common folder structures
        self.category_keywords = {
            'financials': ['financial', 'finance', 'budget', 'accounting', 'statements'],
            'meetings': ['meeting', 'notes', 'agenda', 'minutes', 'quarterly'],
            'projects': ['project', 'initiative', 'implementation'],
            'documents': ['handout', 'template', 'form', 'policy'],
            'behavioral': ['behavioral', 'profile', 'assessment', 'culture'],
            'training': ['training', 'onboarding', 'handbook'],
            'archived': ['archive', 'archived', 'old'],
            'airplane': ['airplane', 'travel', 'dashboard']
        }
    
    def extract_client_info(self, document_path: str) -> Dict[str, Any]:
        """Extract client information from document path"""
        if not document_path:
            return self._create_unknown_client_info(document_path)
            
        # Check for client pattern
        client_match = self.client_pattern.search(document_path)
        
        if client_match:
            client_name = client_match.group(1).strip()
            pm_initial = client_match.group(2)
            pm_name = self.pm_names.get(pm_initial, "Unknown")
            
            # Extract folder path (everything after client folder)
            client_folder_end = client_match.end()
            remaining_path = document_path[client_folder_end:] if client_folder_end < len(document_path) else ""
            
            # Determine document category
            category = self._determine_document_category(document_path)
            
            return {
                "client_name": client_name,
                "pm_initial": pm_initial,
                "pm_name": pm_name,
                "folder_path": remaining_path,
                "is_client_specific": True,
                "document_category": category
            }
        else:
            # Check if it's an internal/tool folder
            path_lower = document_path.lower()
            for internal_folder in self.internal_folders:
                if f'/{internal_folder}/' in path_lower or path_lower.startswith(f'/{internal_folder}/'):
                    category = self._determine_document_category(document_path)
                    return {
                        "client_name": "Autobahn Internal",
                        "pm_initial": "N/A",
                        "pm_name": "Internal",
                        "folder_path": document_path,
                        "is_client_specific": False,
                        "document_category": category
                    }
            
            # Unknown client pattern
            return self._create_unknown_client_info(document_path)
    
    def _create_unknown_client_info(self, document_path: str) -> Dict[str, Any]:
        """Create metadata for documents that don't match client patterns"""
        return {
            "client_name": "Unknown Client",
            "pm_initial": "N/A",
            "pm_name": "Unknown",
            "folder_path": document_path or "unknown",
            "is_client_specific": False,
            "document_category": "general"
        }
    
    def _determine_document_category(self, path: str) -> str:
        """Determine document category based on path keywords"""
        path_lower = path.lower()
        
        for category, keywords in self.category_keywords.items():
            if any(keyword in path_lower for keyword in keywords):
                return category
                
        return "general"

class DocumentChunker:
    """Chunk documents for RAG processing with enhanced metadata"""
    
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def chunk_document(self, content: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk a document into overlapping segments with metadata"""
        if not content or not content.strip():
            return []
        
        chunks = []
        content = content.strip()
        
        # Simple chunking by character count with word boundaries
        start = 0
        chunk_id = 0
        
        while start < len(content):
            # Calculate end position
            end = start + self.chunk_size
            
            # If we're not at the end, try to break at word boundary
            if end < len(content):
                # Look backwards for a word boundary
                while end > start and content[end] not in [' ', '\n', '\t', '.', '!', '?']:
                    end -= 1
                
                # If we couldn't find a good break, use the original end
                if end == start:
                    end = start + self.chunk_size
            
            chunk_text = content[start:end].strip()
            
            if chunk_text:  # Only add non-empty chunks
                chunk_metadata = metadata.copy()
                chunk_metadata.update({
                    "chunk_id": chunk_id,
                    "chunk_text": chunk_text,
                    "chunk_length": len(chunk_text),
                    "chunk_word_count": len(chunk_text.split()),
                    "chunk_start_position": start,
                    "chunk_end_position": end,
                    "total_document_length": len(content)
                })
                
                chunks.append(chunk_metadata)
                chunk_id += 1
            
            # Move start position (with overlap)
            start = max(start + 1, end - self.chunk_overlap)
            
            # Prevent infinite loop
            if start >= len(content):
                break
        
        return chunks
    
    def chunk_excel_document(self, excel_data: Dict[str, Any], metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk Excel document preserving sheet structure"""
        if excel_data.get("type") != "excel_sheets" or not excel_data.get("sheets"):
            return []
        
        chunks = []
        chunk_id = 0
        
        for sheet_name, sheet_content in excel_data["sheets"].items():
            if not sheet_content or not sheet_content.strip():
                continue
            
            # Create sheet-specific metadata
            sheet_metadata = metadata.copy()
            sheet_metadata.update({
                "sheet_name": sheet_name,
                "sheet_content_length": len(sheet_content),
                "sheet_word_count": len(sheet_content.split())
            })
            
            # Check if sheet title contains client name
            if EnhancedExcelProcessor:
                try:
                    processor = EnhancedExcelProcessor()
                    sheet_client_info = processor.sheet_client_detector.detect_client_from_sheet_title(sheet_name)
                    if sheet_client_info and sheet_client_info.get("detected_client"):
                        sheet_metadata["sheet_client_name"] = sheet_client_info["detected_client"]
                        sheet_metadata["sheet_pm_name"] = sheet_client_info.get("pm_name", "Unknown")
                        sheet_metadata["client_detection_source"] = "sheet_title"
                except Exception as e:
                    logging.warning(f"Failed to detect client from sheet title '{sheet_name}': {e}")
            
            # Chunk the sheet content
            sheet_chunks = self.chunk_document(sheet_content, sheet_metadata)
            
            # Update chunk IDs to be globally unique
            for chunk in sheet_chunks:
                chunk["chunk_id"] = chunk_id
                chunk["sheet_chunk_id"] = chunk["chunk_id"]  # Also preserve sheet-relative ID
                chunk_id += 1
            
            chunks.extend(sheet_chunks)
        
        return chunks

def main(req: func.HttpRequest) -> func.HttpResponse:
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
        """Initialize Azure services and enhanced processing components"""
        try:
            # Load environment variables
            self.storage_connection = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            self.key_vault_url = os.environ.get('AZURE_KEY_VAULT_URL')
            self.doc_intelligence_endpoint = os.environ.get('DOCUMENT_INTELLIGENCE_ENDPOINT')
            self.doc_intelligence_key = os.environ.get('DOCUMENT_INTELLIGENCE_KEY')
            self.pii_function_url = os.environ.get('PII_FUNCTION_URL')
            self.pii_function_key = os.environ.get('PII_FUNCTION_KEY')
            
            # Cost Control Settings
            self.test_mode = os.environ.get('TEST_MODE', 'false').lower() == 'true'
            self.max_documents_per_run = int(os.environ.get('MAX_DOCUMENTS_PER_RUN', '50'))
            self.max_file_size_mb = int(os.environ.get('MAX_FILE_SIZE_MB', '100'))
            self.skip_doc_intelligence_for_large_files = True
            self.max_concurrent_docs = int(os.environ.get('MAX_CONCURRENT_DOCS', '10'))
            
            # Initialize Azure clients
            self.storage_client = BlobServiceClient.from_connection_string(self.storage_connection)
            self.key_vault_client = SecretClient(vault_url=self.key_vault_url, credential=DefaultAzureCredential())
            self.doc_intelligence_client = DocumentAnalysisClient(
                endpoint=self.doc_intelligence_endpoint,
                credential=AzureKeyCredential(self.doc_intelligence_key)
            )
            
            # Get Graph API token
            self.graph_token = self._get_graph_token()
            
            # Initialize enhanced processing components
            self.client_extractor = ClientMetadataExtractor()
            self.chunker = DocumentChunker()
            
            # Initialize enhanced Excel processor if available
            if EnhancedExcelProcessor:
                try:
                    self.enhanced_excel_processor = EnhancedExcelProcessor()
                    logging.info("Enhanced Excel processor initialized successfully")
                except Exception as e:
                    logging.warning(f"Failed to initialize enhanced Excel processor: {e}")
                    self.enhanced_excel_processor = None
            else:
                self.enhanced_excel_processor = None
            
            # Initialize modern enhanced processor for production-ready processing
            try:
                from src.core.enhanced_document_processor import EnhancedDocumentProcessor
                self.modern_processor = EnhancedDocumentProcessor(
                    storage_connection=self.storage_connection,
                    doc_intelligence_endpoint=self.doc_intelligence_endpoint,
                    doc_intelligence_key=self.doc_intelligence_key,
                    max_concurrent_docs=self.max_concurrent_docs
                )
                self.use_modern_processor = os.environ.get('USE_ENHANCED_PROCESSOR', 'true').lower() == 'true'
                logging.info(f"Modern enhanced processor initialized - Usage: {'Enabled' if self.use_modern_processor else 'Disabled'}")
            except ImportError as e:
                logging.warning(f"Modern processor not available: {e}")
                self.modern_processor = None
                self.use_modern_processor = False
            
            # Define supported file types with size preferences
            self.supported_extensions = {'.pdf', '.docx', '.doc', '.xlsx', '.xls', '.pptx', '.ppt', '.txt'}
            self.lightweight_extensions = {'.txt', '.docx', '.doc'}
            
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
                "processing_start_time": datetime.datetime.utcnow(),
                "enhanced_processing_enabled": self.use_modern_processor
            }
            
            logging.info(f'Enhanced DocumentProcessor initialized - Test Mode: {self.test_mode}, Max Docs: {self.max_documents_per_run}, Enhanced: {self.use_modern_processor}')
            
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
        """Process documents from a specific SharePoint site
        
        Args:
            site_name: Name of the SharePoint site
            folder_path: Optional folder path to target
        """
        
        site_results = {
            "site_name": site_name,
            "folder_path": folder_path or "/",
            "test_mode": self.test_mode,
            "documents_found": 0,
            "documents_processed": 0,
            "documents_skipped": 0,
            "documents_failed": 0,
            "processing_errors": [],
            "cost_estimate": 0.0,
            "total_processing_time_seconds": 0
        }
        
        processing_start = datetime.datetime.utcnow()
        
        try:
            # Get site ID
            site_id = self._get_site_id(site_name)
            if not site_id:
                logging.warning(f'Site not found: {site_name}')
                return site_results
            
            # Get documents from site
            documents = self._get_site_documents(site_id, folder_path)
            site_results["documents_found"] = len(documents)
            logging.info(f"Found {len(documents)} documents in site '{site_name}'")
            
            # Process each document
            for doc in documents:
                if self.processing_stats["documents_processed_this_run"] >= self.max_documents_per_run:
                    logging.info(f'Hit processing limit ({self.max_documents_per_run}), stopping')
                    break
                
                try:
                    result = self._process_single_document_with_cost_control(doc)
                    
                    action = result.get("action", "unknown")
                    cost = result.get("cost_estimate", 0.0)
                    
                    if action in ["processed", "quarantined", "flagged"]:
                        self.processing_stats["documents_processed_this_run"] += 1
                        self.processing_stats["total_cost_estimate"] += cost
                        site_results["cost_estimate"] += cost
                    
                    if action == "processed":
                        site_results["documents_processed"] += 1
                    elif action in ["skipped", "skipped_size_limit", "skipped_extraction_failed"]:
                        site_results["documents_skipped"] += 1
                    else:
                        site_results["documents_failed"] += 1
                        site_results["processing_errors"].append({
                            "file": result.get("path", "unknown"),
                            "reason": result.get("reason", "unknown"),
                            "extension": result.get("extension", "unknown")
                        })
                
                except Exception as e:
                    logging.error(f'Error processing document {doc.get("name", "unknown")}: {str(e)}')
                    site_results["documents_failed"] += 1
                    site_results["processing_errors"].append({
                        "file": doc.get("path", doc.get("name", "unknown")),
                        "reason": f"critical_error: {str(e)}",
                        "extension": doc.get("extension", "unknown")
                    })
            
            # Calculate processing time
            processing_end = datetime.datetime.utcnow()
            site_results["total_processing_time_seconds"] = round((processing_end - processing_start).total_seconds(), 2)
            
            logging.info(f'Site {site_name} processing complete: {site_results["documents_processed"]} processed, {site_results["documents_skipped"]} skipped, {site_results["documents_failed"]} failed')
            
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
    
    def _get_site_documents(self, site_id: str, folder_path: str = None) -> List[Dict[str, Any]]:
        """Get documents from a SharePoint site"""
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
            
            for drive in drives_data['value']:
                drive_id = drive['id']
                
                if folder_path:
                    # Clean up folder path
                    clean_folder_path = folder_path
                    for current_drive in drives_data['value']:
                        if folder_path.startswith(f"{current_drive['name']}/"):
                            clean_folder_path = folder_path[len(f"{current_drive['name']}/"):]
                            break
                    
                    documents.extend(self._get_drive_documents(drive_id, clean_folder_path))
                else:
                    documents.extend(self._get_drive_documents(drive_id))
            
            return documents
            
        except Exception as e:
            logging.error(f'Error getting documents from site {site_id}: {str(e)}')
            return []
    
    def _get_drive_documents(self, drive_id: str, folder_path: str = None) -> List[Dict[str, Any]]:
        """Get documents from a specific drive"""
        try:
            headers = {'Authorization': f'Bearer {self.graph_token}'}
            documents = []
            
            if folder_path:
                url = self._build_folder_url(drive_id, folder_path)
            else:
                url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children?$top=100'
            
            while url and len(documents) < 1000:  # Safety limit
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                for item in data.get('value', []):
                    if 'file' in item and item.get('name'):
                        extension = os.path.splitext(item['name'])[1].lower()
                        if extension in self.supported_extensions:
                            documents.append({
                                'id': item['id'],
                                'name': item['name'],
                                'path': item.get('parentReference', {}).get('path', '') + '/' + item['name'],
                                'size': item['size'],
                                'download_url': item.get('@microsoft.graph.downloadUrl', ''),
                                'extension': extension
                            })
                
                url = data.get('@odata.nextLink')
            
            return documents
            
        except Exception as e:
            logging.error(f'Error getting drive documents: {str(e)}')
            return []
    
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
    
    def _get_specific_file(self, site_id: str, folder_path: str, file_name: str) -> Dict[str, Any]:
        """Get a specific file from SharePoint"""
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
                        'extension': os.path.splitext(file_name)[1].lower()
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
    
    def _process_single_document_with_cost_control(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single document with enhanced functionality"""
        doc_name = doc['name']
        doc_path = doc.get('path', doc_name)
        doc_extension = doc.get('extension', 'unknown')
        doc_size = doc.get('size', 0)
        
        # Use modern processor if available and enabled
        if self.use_modern_processor and self.modern_processor:
            return self._process_with_modern_processor(doc)
        
        # Use legacy processing approach
        return self._process_single_document_legacy(doc)
    
    def _process_single_document_legacy(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Legacy document processing method (renamed from original)"""
        doc_name = doc['name']
        doc_path = doc.get('path', doc_name)
        doc_extension = doc.get('extension', 'unknown')
        doc_size = doc.get('size', 0)
        
        try:
            logging.info(f'Processing document: {doc_path} (legacy processor)')
            
            # Generate document ID
            doc_id = hashlib.md5(doc_path.encode()).hexdigest()
            full_doc_path = f"sharepoint://{doc_path}"
            
            # Download document
            doc_content = self._download_document(doc['download_url'])
            
            # Extract text content
            extracted_content = self._extract_text_content(doc_content, doc_extension, doc_name)
            
            # Extract client metadata
            client_info = self.client_extractor.extract_client_info(full_doc_path)
            
            # Handle Excel files differently - they return structured data
            if doc_extension.lower() in ['.xlsx', '.xls'] and isinstance(extracted_content, dict):
                if extracted_content.get("type") == "excel_sheets":
                    # Use Excel-specific chunking for sheet-based processing
                    chunks = self.chunker.chunk_excel_document(extracted_content, {
                        "id": doc_id,
                        "name": doc_name,
                        "document_path": full_doc_path,
                        "filename": doc_name,
                        "size": doc_size,
                        "extension": doc_extension,
                        "processed_timestamp": datetime.datetime.utcnow().isoformat(),
                        "status": "ready_for_rag",
                        "client_name": client_info["client_name"],
                        "pm_initial": client_info["pm_initial"],
                        "pm_name": client_info["pm_name"],
                        "document_category": client_info["document_category"],
                        "is_client_specific": client_info["is_client_specific"],
                        "processing_method": "excel_sheet_extraction",
                        "folder_depth": len([p for p in full_doc_path.split('/') if p]),
                        "file_extension": doc_extension.lower()
                    })
                    
                    if chunks:
                        # Store chunks in blob storage
                        try:
                            self._store_processed_chunks(chunks)
                            return {
                                "action": "processed",
                                "reason": "excel_enhanced_processing",
                                "path": doc_path,
                                "extension": doc_extension,
                                "cost_estimate": 0.0,
                                "chunks_created": len(chunks),
                                "total_sheets": extracted_content["total_sheets"]
                            }
                        except Exception as e:
                            logging.error(f'Error storing chunks for {doc_path}: {str(e)}')
                            return {
                                "action": "error",
                                "reason": f"chunk_storage_failed: {str(e)}",
                                "path": doc_path,
                                "extension": doc_extension,
                                "cost_estimate": 0.0
                            }
                else:
                    # Excel extraction failed, skip
                    return {
                        "action": "skipped",
                        "reason": "excel_extraction_failed",
                        "path": doc_path,
                        "extension": doc_extension,
                        "error": extracted_content.get("error", "Unknown Excel error")
                    }
            else:
                # Handle regular text-based documents
                if not extracted_content or len(extracted_content.strip()) < 50:
                    return {
                        "action": "skipped",
                        "reason": "no_extractable_content",
                        "path": doc_path,
                        "extension": doc_extension
                    }
                
                # Create enhanced metadata for regular documents
                enhanced_metadata = {
                    "id": doc_id,
                    "name": doc_name,
                    "document_path": full_doc_path,
                    "filename": doc_name,
                    "size": doc_size,
                    "extension": doc_extension,
                    "processed_timestamp": datetime.datetime.utcnow().isoformat(),
                    "status": "ready_for_rag",
                    "client_name": client_info["client_name"],
                    "pm_initial": client_info["pm_initial"],
                    "pm_name": client_info["pm_name"],
                    "document_category": client_info["document_category"],
                    "is_client_specific": client_info["is_client_specific"],
                    "content_length": len(extracted_content),
                    "word_count": len(extracted_content.split()),
                    "character_count": len(extracted_content),
                    "processing_method": "azure_document_intelligence",
                    "folder_depth": len([p for p in full_doc_path.split('/') if p]),
                    "file_extension": doc_extension.lower()
                }
                
                # Chunk document for RAG using regular chunking
                chunks = self.chunker.chunk_document(extracted_content, enhanced_metadata)
                
                if chunks:
                    # Store chunks in blob storage
                    try:
                        self._store_processed_chunks(chunks)
                        return {
                            "action": "processed",
                            "reason": "enhanced_processing",
                            "path": doc_path,
                            "extension": doc_extension,
                            "cost_estimate": 0.0,
                            "chunks_created": len(chunks),
                            "content_length": len(extracted_content)
                        }
                    except Exception as e:
                        logging.error(f'Error storing chunks for {doc_path}: {str(e)}')
                        return {
                            "action": "error",
                            "reason": f"chunk_storage_failed: {str(e)}",
                            "path": doc_path,
                            "extension": doc_extension,
                            "cost_estimate": 0.0
                        }
                else:
                    return {
                        "action": "skipped",
                        "reason": "no_chunks_created",
                        "path": doc_path,
                        "extension": doc_extension
                    }
            
        except Exception as e:
            logging.error(f'Error processing document {doc_path}: {str(e)}')
            return {
                "action": "error",
                "reason": f"processing_failed: {str(e)}",
                "path": doc_path,
                "extension": doc_extension,
                "cost_estimate": 0.0
            }
    
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
    
    def _extract_text_content(self, doc_content: bytes, extension: str, filename: str) -> str:
        """Extract text content from document using appropriate method"""
        try:
            extension_lower = extension.lower()
            
            # Handle Excel files with enhanced processor if available
            if extension_lower in ['.xlsx', '.xls']:
                if self.enhanced_excel_processor:
                    try:
                        return self.enhanced_excel_processor.extract_excel_content(doc_content, filename)
                    except Exception as e:
                        logging.warning(f'Enhanced Excel processing failed for {filename}: {e}')
                        # Fall back to basic Excel processing
                        return self._extract_excel_basic(doc_content)
                else:
                    return self._extract_excel_basic(doc_content)
            
            # Handle PowerPoint files
            elif extension_lower in ['.pptx', '.ppt']:
                return self._extract_powerpoint_content(doc_content)
            
            # Handle text files
            elif extension_lower == '.txt':
                return doc_content.decode('utf-8', errors='ignore')
            
            # Use Azure Document Intelligence for PDF, Word documents
            elif extension_lower in ['.pdf', '.docx', '.doc']:
                return self._extract_using_document_intelligence(doc_content)
            
            else:
                logging.warning(f'Unsupported file type: {extension_lower}')
                return ""
                
        except Exception as e:
            logging.error(f'Error extracting text from {filename}: {str(e)}')
            return ""
    
    def _extract_excel_basic(self, doc_content: bytes) -> str:
        """Basic Excel extraction without enhanced processing"""
        try:
            workbook = openpyxl.load_workbook(BytesIO(doc_content), data_only=True)
            all_text = []
            
            for sheet_name in workbook.sheetnames:
                sheet = workbook[sheet_name]
                sheet_text = [f"Sheet: {sheet_name}"]
                
                for row in sheet.iter_rows(values_only=True):
                    row_text = []
                    for cell in row:
                        if cell is not None:
                            row_text.append(str(cell))
                    if row_text:
                        sheet_text.append(' | '.join(row_text))
                
                all_text.append('\n'.join(sheet_text))
            
            return '\n\n'.join(all_text)
            
        except Exception as e:
            logging.error(f'Error in basic Excel extraction: {str(e)}')
            return ""
    
    def _extract_powerpoint_content(self, doc_content: bytes) -> str:
        """Extract text from PowerPoint presentations"""
        try:
            prs = Presentation(BytesIO(doc_content))
            all_text = []
            
            for i, slide in enumerate(prs.slides):
                slide_text = [f"Slide {i+1}:"]
                
                for shape in slide.shapes:
                    if hasattr(shape, "text") and shape.text.strip():
                        slide_text.append(shape.text.strip())
                
                all_text.append('\n'.join(slide_text))
            
            return '\n\n'.join(all_text)
            
        except Exception as e:
            logging.error(f'Error extracting PowerPoint content: {str(e)}')
            return ""
    
    def _extract_using_document_intelligence(self, doc_content: bytes) -> str:
        """Extract text using Azure Document Intelligence"""
        try:
            # Use Document Intelligence to extract text
            poller = self.doc_intelligence_client.begin_analyze_document(
                "prebuilt-read", 
                document=doc_content
            )
            result = poller.result()
            
            # Extract text content
            text_content = []
            for page in result.pages:
                for line in page.lines:
                    text_content.append(line.content)
            
            return '\n'.join(text_content)
            
        except Exception as e:
            logging.error(f'Error using Document Intelligence: {str(e)}')
            return ""
    
    def _store_processed_chunks(self, chunks: List[Dict[str, Any]]) -> None:
        """Store processed chunks in Azure Blob Storage"""
        try:
            container_name = "jennifur-processed"
            
            # Create container client
            container_client = self.storage_client.get_container_client(container_name)
            
            # Ensure container exists
            try:
                container_client.get_container_properties()
            except Exception:
                container_client.create_container()
            
            # Store each chunk as a separate blob
            for chunk in chunks:
                chunk_id = chunk.get("chunk_id", "unknown")
                doc_id = chunk.get("id", "unknown")
                blob_name = f"{doc_id}/chunk_{chunk_id}.json"
                
                # Upload chunk as JSON
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(
                    json.dumps(chunk, indent=2), 
                    overwrite=True,
                    content_type="application/json"
                )
            
            logging.info(f"Stored {len(chunks)} chunks in blob storage")
            
        except Exception as e:
            logging.error(f'Error storing chunks in blob storage: {str(e)}')
            raise
    
    def _process_with_modern_processor(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Process document using the modern async processor"""
        try:
            import asyncio
            
            # Create event loop if none exists (for Azure Functions)
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Run async processing
            result = loop.run_until_complete(
                self.modern_processor.process_single_document(doc)
            )
            
            # Convert modern ProcessingResult to legacy format
            return {
                "action": "processed" if result.status.value == "completed" else result.status.value,
                "reason": "enhanced_modern_processing",
                "path": doc.get('path', doc.get('name', 'unknown')),
                "extension": doc.get('extension', 'unknown'),
                "cost_estimate": 0.0,  # Modern processor doesn't track cost
                "chunks_created": result.chunks_created,
                "processing_time_ms": result.processing_time_ms,
                "confidence_score": result.confidence_score,
                "tokens_processed": result.tokens_processed,
                "error_message": result.error_message
            }
            
        except Exception as e:
            logging.error(f'Modern processor failed, falling back to legacy: {str(e)}')
            
            # Disable modern processor for this session and fallback
            self.use_modern_processor = False
            return self._process_single_document_legacy(doc)
    
    def _process_single_document_legacy(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Legacy document processing method (renamed from original)"""
        doc_name = doc['name']
        doc_path = doc.get('path', doc_name)
        doc_extension = doc.get('extension', 'unknown')
        doc_size = doc.get('size', 0)
        
        try:
            logging.info(f'Processing document: {doc_path} (legacy processor)')
            
            # Generate document ID
            doc_id = hashlib.md5(doc_path.encode()).hexdigest()
            full_doc_path = f"sharepoint://{doc_path}"
            
            # Download document
            doc_content = self._download_document(doc['download_url'])
            
            # Extract text content
            extracted_content = self._extract_text_content(doc_content, doc_extension, doc_name)
            
            # Extract client metadata
            client_info = self.client_extractor.extract_client_info(full_doc_path)