import json
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import os
import datetime
from typing import Dict, Any, List
import re

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to repair metadata for already ingested documents
    """
    logging.info('Metadata repair function triggered')
    
    try:
        req_body = req.get_json()
        
        # Get optional parameters
        dry_run = req_body.get('dry_run', True) if req_body else True  # Default to dry run for safety
        max_documents = req_body.get('max_documents', 100) if req_body else 100
        
        processor = MetadataRepairProcessor()
        result = processor.repair_metadata(dry_run=dry_run, max_documents=max_documents)
        
        return func.HttpResponse(
            json.dumps(result, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f'Error in metadata repair function: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

class MetadataRepairProcessor:
    def __init__(self):
        """Initialize Azure services for metadata repair"""
        try:
            # Load environment variables
            self.storage_connection = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
            
            # Initialize Azure clients
            self.storage_client = BlobServiceClient.from_connection_string(self.storage_connection)
            
            logging.info('MetadataRepairProcessor initialized successfully')
            
        except Exception as e:
            logging.error(f'Failed to initialize MetadataRepairProcessor: {str(e)}')
            raise

    def repair_metadata(self, dry_run: bool = True, max_documents: int = 100) -> Dict[str, Any]:
        """Repair metadata for already ingested documents"""
        
        repair_results = {
            "dry_run": dry_run,
            "max_documents": max_documents,
            "documents_processed": 0,
            "documents_updated": 0,
            "documents_skipped": 0,
            "documents_failed": 0,
            "processing_errors": [],
            "sample_updates": [],
            "processing_time_seconds": 0
        }
        
        processing_start = datetime.datetime.utcnow()
        
        try:
            # Get all chunk blobs from jennifur-processed container
            container_client = self.storage_client.get_container_client("jennifur-processed")
            blobs = list(container_client.list_blobs())
            
            logging.info(f"Found {len(blobs)} total blobs in jennifur-processed container")
            
            # Filter for chunk files (exclude summary files)
            chunk_blobs = [blob for blob in blobs if blob.name.endswith('.json') and not blob.name.endswith('_summary.json')]
            
            # Limit to max_documents for processing
            chunk_blobs_to_process = chunk_blobs[:max_documents]
            
            logging.info(f"Processing {len(chunk_blobs_to_process)} chunk files (dry_run: {dry_run})")
            
            for blob in chunk_blobs_to_process:
                try:
                    result = self._repair_single_chunk(blob.name, dry_run)
                    
                    repair_results["documents_processed"] += 1
                    
                    if result["updated"]:
                        repair_results["documents_updated"] += 1
                        
                        # Add to sample updates (first 5)
                        if len(repair_results["sample_updates"]) < 5:
                            repair_results["sample_updates"].append({
                                "chunk_id": blob.name,
                                "old_metadata": result["old_metadata"],
                                "new_metadata": result["new_metadata"]
                            })
                    else:
                        repair_results["documents_skipped"] += 1
                        
                except Exception as e:
                    repair_results["documents_failed"] += 1
                    repair_results["processing_errors"].append({
                        "chunk_id": blob.name,
                        "error": str(e)
                    })
                    logging.error(f"Error processing chunk {blob.name}: {str(e)}")
            
            # Calculate total processing time
            processing_end = datetime.datetime.utcnow()
            repair_results["processing_time_seconds"] = round((processing_end - processing_start).total_seconds(), 2)
            
            logging.info(f'Metadata repair completed: {repair_results["documents_updated"]} updated, {repair_results["documents_skipped"]} skipped, {repair_results["documents_failed"]} failed')
            
            return repair_results
            
        except Exception as e:
            processing_end = datetime.datetime.utcnow()
            repair_results["processing_time_seconds"] = round((processing_end - processing_start).total_seconds(), 2)
            error_msg = f'Error during metadata repair: {str(e)}'
            logging.error(error_msg)
            repair_results["processing_errors"].append({
                "error": error_msg
            })
            return repair_results

    def _repair_single_chunk(self, blob_name: str, dry_run: bool = True) -> Dict[str, Any]:
        """Repair metadata for a single chunk file"""
        
        try:
            # Download the chunk data
            blob_client = self.storage_client.get_blob_client(
                container="jennifur-processed",
                blob=blob_name
            )
            
            chunk_data = json.loads(blob_client.download_blob().readall().decode('utf-8'))
            
            # Extract current metadata
            old_metadata = {
                "client_name": chunk_data.get("client_name"),
                "pm_initial": chunk_data.get("pm_initial"), 
                "pm_name": chunk_data.get("pm_name"),
                "is_client_specific": chunk_data.get("is_client_specific"),
                "has_client_folder": chunk_data.get("has_client_folder"),
                "document_category": chunk_data.get("document_category")
            }
            
            # Extract new metadata from document path
            doc_path = chunk_data.get("document_path", "")
            new_client_metadata = self._extract_client_metadata_from_path(doc_path)
            new_document_category = self._extract_document_category_from_path(doc_path)
            
            # Determine if update is needed
            needs_update = False
            new_metadata = old_metadata.copy()
            
            if new_client_metadata:
                # Update client-specific metadata
                new_metadata.update({
                    "client_name": new_client_metadata['client_name'],
                    "pm_initial": new_client_metadata['pm_code'],
                    "pm_name": new_client_metadata['pm_name'],
                    "is_client_specific": True,
                    "has_client_folder": True
                })
            else:
                # No client metadata found - set to internal
                new_metadata.update({
                    "client_name": "Autobahn Internal",
                    "pm_initial": "N/A",
                    "pm_name": "N/A", 
                    "is_client_specific": False,
                    "has_client_folder": False
                })
            
            # Update document category if found
            if new_document_category:
                new_metadata["document_category"] = new_document_category
            
            needs_update = (old_metadata != new_metadata)
            
            # Update the chunk data if needed
            if needs_update and not dry_run:
                # Update the chunk data with new metadata
                chunk_data.update(new_metadata)
                chunk_data["metadata_updated_timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
                
                # Upload the updated chunk
                blob_client.upload_blob(
                    json.dumps(chunk_data, indent=2),
                    overwrite=True,
                    content_type='application/json'
                )
                
                logging.info(f"Updated metadata for {blob_name}: {old_metadata['client_name']} -> {new_metadata['client_name']}")
            
            return {
                "updated": needs_update,
                "old_metadata": old_metadata,
                "new_metadata": new_metadata if needs_update else None
            }
            
        except Exception as e:
            logging.error(f"Error repairing chunk {blob_name}: {str(e)}")
            raise

    def _extract_client_metadata_from_path(self, doc_path: str) -> Dict[str, Any]:
        """Extract client and PM metadata from SharePoint folder path, prioritizing main client folder"""
        
        logging.info(f'🏢 Extracting client metadata from path: {doc_path}')
        
        # First, look for the main client folder pattern: Client Name (PM-X)/
        # This ensures we extract from the main folder, not subfolders like "Archived"
        client_folder_pattern = r'([^/]+)\s*\(PM-([A-Za-z])\)/'
        match = re.search(client_folder_pattern, doc_path, re.IGNORECASE)
        
        if match:
            client_name = match.group(1).strip()
            pm_code = match.group(2).upper()
            
            logging.info(f'✅ Found main client folder: "{client_name}" with PM-{pm_code}')
        else:
            # Fallback: look for client pattern anywhere in path (less preferred)
            fallback_patterns = [
                r'([^/]+)\s*\(PM-([A-Z])\)',  # Standard format
                r'([^/]+)\s*\(PM-([a-zA-Z])\)',  # Case insensitive PM code
                r'(.*?)\s*\(PM-([A-Z])\)',  # Any text before PM pattern
            ]
            
            for pattern in fallback_patterns:
                match = re.search(pattern, doc_path, re.IGNORECASE)
                if match:
                    client_name = match.group(1).strip()
                    pm_code = match.group(2).upper()
                    logging.info(f'⚠️ Using fallback pattern for client: "{client_name}" with PM-{pm_code}')
                    break
            else:
                logging.warning(f'❌ No client metadata found in path: {doc_path}')
                return None
        
        # Map PM codes to names
        pm_mapping = {
            'C': 'Caleb',
            'K': 'Katherine', 
            'S': 'Sam'
        }
        
        pm_name = pm_mapping.get(pm_code, pm_code)
        
        # Clean up client name (remove leading/trailing slashes and whitespace)
        client_name = client_name.strip('/ ')
        
        result = {
            'client_name': client_name,
            'pm_code': pm_code,
            'pm_name': pm_name,
            'confidence_score': 0.9
        }
        
        logging.info(f'🎯 Extracted client metadata: {result}')
        return result

    def _extract_document_category_from_path(self, doc_path: str) -> str:
        """Extract document category from the main category folder, ignoring subfolders"""
        
        logging.info(f'🔍 Extracting document category from path: {doc_path}')
        
        # Find the client folder pattern and extract the main category folder
        # Pattern: Client (PM-X)/MainCategoryFolder/[optional subfolders]/file
        client_pattern = r'([^/]+)\s*\(PM-[A-Z]\)/([^/]+)'
        match = re.search(client_pattern, doc_path, re.IGNORECASE)
        
        if match:
            main_category_folder = match.group(2).strip()
            logging.info(f'📁 Found main category folder: "{main_category_folder}"')
            
            # Extract category from patterns like "_08. Financials" -> "financials"
            # Pattern: optional underscore/number, dot, then category name
            category_pattern = r'^_?\d+\.\s*(.+)'
            category_match = re.search(category_pattern, main_category_folder, re.IGNORECASE)
            
            if category_match:
                category_text = category_match.group(1).strip()
                # Clean and normalize the category
                # Remove special characters, convert to lowercase, replace spaces with underscores
                clean_category = re.sub(r'[^\w\s]', '', category_text)  # Remove special chars
                clean_category = re.sub(r'\s+', '_', clean_category.strip().lower())  # Spaces to underscores, lowercase
                
                logging.info(f'✅ Extracted document category: "{clean_category}" from main folder "{category_text}"')
                return clean_category
            else:
                # If no number pattern, use the folder name directly (cleaned)
                clean_category = re.sub(r'[^\w\s]', '', main_category_folder)
                clean_category = re.sub(r'\s+', '_', clean_category.strip().lower())
                
                if clean_category:
                    logging.info(f'✅ Using cleaned main folder name as category: "{clean_category}"')
                    return clean_category
        
        logging.warning(f'⚠️ Could not extract document category from path: {doc_path}')
        return "general"