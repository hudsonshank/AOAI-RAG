#!/usr/bin/env python3
"""
Test Document Reprocessing - Process first 50 old-format documents
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import enhanced document processor
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from src.utils.enhanced_document_processor import EnhancedDocumentProcessor

class TestDocumentReprocessor:
    """Test reprocessing on 50 documents"""
    
    def __init__(self):
        self.storage_client = BlobServiceClient.from_connection_string(
            os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        )
        self.container_client = self.storage_client.get_container_client('jennifur-processed')
        self.processor = EnhancedDocumentProcessor()
        
    def find_old_format_documents(self, limit=25):
        """Find first 25 old-format documents"""
        logger.info(f"🔍 Finding first {limit} old-format documents...")
        
        old_docs = []
        
        for blob in self.container_client.list_blobs():
            if len(old_docs) >= limit:
                break
                
            try:
                # Download and parse document
                blob_client = self.storage_client.get_blob_client(
                    container='jennifur-processed', 
                    blob=blob.name
                )
                content = blob_client.download_blob().readall().decode('utf-8')
                doc_data = json.loads(content)
                
                # Check if old format
                has_content = 'content' in doc_data and doc_data['content'] is not None
                has_chunk = 'chunk' in doc_data and doc_data['chunk'] is not None
                has_enhanced_metadata = all(field in doc_data for field in [
                    'client_name', 'pm_initial', 'document_category'
                ])
                
                if has_content and not has_chunk and not has_enhanced_metadata:
                    old_docs.append({
                        "blob_name": blob.name,
                        "filename": doc_data.get("filename", "unknown"),
                        "document_path": doc_data.get("document_path", "unknown"),
                        "last_modified": blob.last_modified.isoformat()
                    })
                    
                    if len(old_docs) % 10 == 0:
                        logger.info(f"  Found {len(old_docs)} old-format documents so far...")
                        
            except Exception as e:
                logger.warning(f"Error analyzing {blob.name}: {str(e)}")
        
        logger.info(f"📄 Found {len(old_docs)} old-format documents to test")
        return old_docs
    
    def reprocess_document(self, blob_name: str) -> dict:
        """Reprocess a single document with TEST prefix"""
        try:
            # Download original document
            blob_client = self.storage_client.get_blob_client(
                container='jennifur-processed', 
                blob=blob_name
            )
            content = blob_client.download_blob().readall().decode('utf-8')
            old_doc_data = json.loads(content)
            
            # Extract original content and metadata
            document_content = old_doc_data.get('content', '')
            document_path = old_doc_data.get('document_path', '')
            filename = old_doc_data.get('filename', blob_name.replace('.json', ''))
            
            if not document_content:
                return {"status": "skipped", "reason": "no_content", "blob_name": blob_name}
            
            # Process with enhanced processor
            enhanced_metadata = self.processor.process_document_for_indexing(
                document_content=document_content,
                document_path=document_path,
                filename=filename,
                additional_metadata=old_doc_data.get('metadata', {})
            )
            
            # Convert to chunks
            chunks = self._create_chunks_from_content(document_content, enhanced_metadata)
            
            # Store test chunks with TEST_ prefix
            stored_chunks = []
            for i, chunk_content in enumerate(chunks):
                # Start with enhanced metadata but REMOVE the full content field
                chunk_data = {**enhanced_metadata}
                
                # Remove the old 'content' field - we only want 'chunk'
                chunk_data.pop('content', None)
                
                # Add chunk-specific fields
                chunk_data.update({
                    "chunk": chunk_content,
                    "chunk_id": f"TEST_{enhanced_metadata.get('document_id', blob_name.replace('.json', ''))}_{i}",
                    "chunk_index": i,
                    "parent_id": f"TEST_{enhanced_metadata.get('document_id', blob_name.replace('.json', ''))}",
                    "processing_method": "test_reprocessed_enhanced",
                    "original_blob_name": blob_name
                })
                
                # Store test chunk
                test_chunk_name = f"TEST_{chunk_data['chunk_id']}.json"
                self._store_chunk(test_chunk_name, chunk_data)
                stored_chunks.append(test_chunk_name)
            
            return {
                "status": "success",
                "blob_name": blob_name,
                "chunks_created": len(stored_chunks),
                "stored_chunks": stored_chunks,
                "enhanced_metadata": {
                    "client_name": enhanced_metadata.get("client_name"),
                    "pm_initial": enhanced_metadata.get("pm_initial"),
                    "document_category": enhanced_metadata.get("document_category")
                }
            }
            
        except Exception as e:
            logger.error(f"Error reprocessing {blob_name}: {str(e)}")
            return {"status": "error", "blob_name": blob_name, "error": str(e)}
    
    def _create_chunks_from_content(self, content: str, metadata: dict) -> list:
        """Create chunks from content"""
        chunk_size = 1000
        min_chunk_size = 100
        
        if len(content) < min_chunk_size:
            return [content]
        
        chunks = []
        sentences = content.split('. ')
        
        current_chunk = ""
        for sentence in sentences:
            if len(current_chunk) + len(sentence) > chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                # Start new chunk with overlap
                overlap_words = current_chunk.split()[-20:]
                current_chunk = " ".join(overlap_words) + " " + sentence
            else:
                current_chunk += sentence + ". " if current_chunk else sentence + ". "
        
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _store_chunk(self, blob_name: str, chunk_data: dict):
        """Store chunk with TEST prefix"""
        blob_client = self.storage_client.get_blob_client(
            container='jennifur-processed',
            blob=blob_name
        )
        
        blob_client.upload_blob(
            json.dumps(chunk_data, indent=2),
            overwrite=True
        )
    
    def cleanup_test_documents(self):
        """Clean up TEST_ prefixed documents"""
        logger.info("🧹 Cleaning up previous test documents...")
        
        test_blobs = []
        for blob in self.container_client.list_blobs():
            if blob.name.startswith('TEST_'):
                test_blobs.append(blob.name)
        
        for blob_name in test_blobs:
            try:
                blob_client = self.storage_client.get_blob_client(
                    container='jennifur-processed',
                    blob=blob_name
                )
                blob_client.delete_blob()
                logger.debug(f"  Deleted {blob_name}")
            except Exception as e:
                logger.warning(f"Could not delete {blob_name}: {str(e)}")
        
        logger.info(f"✅ Cleaned up {len(test_blobs)} test documents")

def main():
    """Run test reprocessing"""
    logger.info("🧪 Starting Test Document Reprocessing (25 documents)")
    
    reprocessor = TestDocumentReprocessor()
    
    # Cleanup previous tests
    reprocessor.cleanup_test_documents()
    
    # Find old documents
    old_docs = reprocessor.find_old_format_documents(limit=25)
    
    if not old_docs:
        logger.info("✅ No old-format documents found!")
        return
    
    # Process documents
    logger.info(f"🔄 Processing {len(old_docs)} documents...")
    
    results = {
        "total_documents": len(old_docs),
        "successful": 0,
        "failed": 0,
        "skipped": 0,
        "processing_results": [],
        "start_time": datetime.utcnow().isoformat()
    }
    
    for i, doc_info in enumerate(old_docs):
        blob_name = doc_info["blob_name"]
        
        logger.info(f"  Processing {i+1}/{len(old_docs)}: {doc_info['filename'][:50]}...")
        
        result = reprocessor.reprocess_document(blob_name)
        results["processing_results"].append(result)
        
        if result["status"] == "success":
            results["successful"] += 1
            logger.info(f"    ✅ Success: {result['chunks_created']} chunks, client={result['enhanced_metadata']['client_name']}")
        elif result["status"] == "skipped":
            results["skipped"] += 1
            logger.info(f"    ⏭️  Skipped: {result['reason']}")
        else:
            results["failed"] += 1
            logger.info(f"    ❌ Failed: {result['error']}")
    
    results["end_time"] = datetime.utcnow().isoformat()
    
    # Save results
    with open('test_reprocessing_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"\n🎉 TEST REPROCESSING COMPLETE!")
    logger.info(f"   ✅ Success: {results['successful']}")
    logger.info(f"   ❌ Failed: {results['failed']}")
    logger.info(f"   ⏭️ Skipped: {results['skipped']}")
    logger.info(f"📄 Results saved to test_reprocessing_results.json")
    
    if results["successful"] > 0:
        logger.info(f"\n🔍 Next steps:")
        logger.info(f"   1. Check test documents in storage (look for TEST_ prefixed files)")
        logger.info(f"   2. Verify enhanced metadata extraction worked")
        logger.info(f"   3. Run cleanup or proceed with full reprocessing")
        logger.info(f"\n🧹 To cleanup test files: reprocessor.cleanup_test_documents()")

if __name__ == "__main__":
    main()