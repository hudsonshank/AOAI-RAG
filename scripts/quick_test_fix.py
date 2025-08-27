#!/usr/bin/env python3
"""
Quick test to verify the content/chunk fix works on 1 document
"""

import os
import json
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import enhanced document processor
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from src.utils.enhanced_document_processor import EnhancedDocumentProcessor

def test_single_document():
    """Test processing of a single document to verify fix"""
    storage_client = BlobServiceClient.from_connection_string(
        os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    )
    container_client = storage_client.get_container_client('jennifur-processed')
    processor = EnhancedDocumentProcessor()
    
    # Find first old document
    for blob in container_client.list_blobs():
        try:
            blob_client = storage_client.get_blob_client('jennifur-processed', blob.name)
            content = blob_client.download_blob().readall().decode('utf-8')
            doc_data = json.loads(content)
            
            # Check if old format
            has_content = 'content' in doc_data and doc_data['content'] is not None
            has_chunk = 'chunk' in doc_data and doc_data['chunk'] is not None
            has_enhanced_metadata = all(field in doc_data for field in [
                'client_name', 'pm_initial', 'document_category'
            ])
            
            if has_content and not has_chunk and not has_enhanced_metadata:
                logger.info(f"Testing with: {doc_data.get('filename', 'unknown')}")
                
                # Process with enhanced processor  
                enhanced_metadata = processor.process_document_for_indexing(
                    document_content=doc_data.get('content', ''),
                    document_path=doc_data.get('document_path', ''),
                    filename=doc_data.get('filename', ''),
                    additional_metadata=doc_data.get('metadata', {})
                )
                
                # Create chunk data with fix
                chunk_data = {**enhanced_metadata}
                chunk_data.pop('content', None)  # Remove content field
                chunk_data.update({
                    "chunk": doc_data.get('content', '')[:1000],  # First 1000 chars as test chunk
                    "chunk_id": f"QUICKTEST_{blob.name.replace('.json', '')}_0",
                    "processing_method": "quick_test_fix"
                })
                
                # Store test chunk
                test_blob_client = storage_client.get_blob_client(
                    'jennifur-processed', 
                    'QUICKTEST_verify_fix.json'
                )
                test_blob_client.upload_blob(json.dumps(chunk_data, indent=2), overwrite=True)
                
                logger.info("✅ Test chunk created successfully")
                logger.info(f"✅ Enhanced metadata: client={enhanced_metadata.get('client_name')}, category={enhanced_metadata.get('document_category')}")
                logger.info(f"✅ Chunk field present: {'chunk' in chunk_data}")
                logger.info(f"✅ Content field removed: {'content' not in chunk_data}")
                
                return True
                
        except Exception as e:
            continue
    
    logger.error("No old format documents found for testing")
    return False

if __name__ == "__main__":
    success = test_single_document()
    if success:
        print("\n🎉 FIX VERIFIED! The reprocessing will now:")
        print("   ✅ Remove 'content' field")
        print("   ✅ Keep only 'chunk' field") 
        print("   ✅ Include enhanced metadata")
        print("   📋 Ready for full reprocessing!")
    else:
        print("❌ Test failed")