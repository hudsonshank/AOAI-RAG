#!/usr/bin/env python3
"""
Document Reprocessing Script
Identifies and reprocesses old-format documents to use enhanced metadata and chunking
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple
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

class DocumentReprocessor:
    """Reprocess old documents with enhanced metadata and chunking"""
    
    def __init__(self):
        self.storage_client = BlobServiceClient.from_connection_string(
            os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        )
        self.container_client = self.storage_client.get_container_client('jennifur-processed')
        self.processor = EnhancedDocumentProcessor()
        
    def analyze_documents(self) -> Dict[str, Any]:
        """Analyze all documents to identify reprocessing needs"""
        logger.info("🔍 Analyzing all documents in jennifur-processed container...")
        
        analysis = {
            "total_documents": 0,
            "old_format_count": 0,
            "new_format_count": 0,
            "old_format_documents": [],
            "corrupted_documents": [],
            "analysis_timestamp": datetime.utcnow().isoformat()
        }
        
        for blob in self.container_client.list_blobs():
            analysis["total_documents"] += 1
            
            try:
                # Download and parse document
                blob_client = self.storage_client.get_blob_client(
                    container='jennifur-processed', 
                    blob=blob.name
                )
                content = blob_client.download_blob().readall().decode('utf-8')
                doc_data = json.loads(content)
                
                # Analyze format
                format_type = self._analyze_document_format(doc_data)
                
                if format_type == "old_format":
                    analysis["old_format_count"] += 1
                    analysis["old_format_documents"].append({
                        "blob_name": blob.name,
                        "last_modified": blob.last_modified.isoformat(),
                        "size": blob.size,
                        "filename": doc_data.get("filename", "unknown"),
                        "document_path": doc_data.get("document_path", "unknown")
                    })
                elif format_type == "new_format":
                    analysis["new_format_count"] += 1
                    
            except Exception as e:
                analysis["corrupted_documents"].append({
                    "blob_name": blob.name,
                    "error": str(e)
                })
                logger.warning(f"Could not analyze {blob.name}: {str(e)}")
            
            # Progress logging
            if analysis["total_documents"] % 100 == 0:
                logger.info(f"  Analyzed {analysis['total_documents']} documents...")
        
        # Summary
        logger.info(f"📊 Analysis Complete:")
        logger.info(f"   Total documents: {analysis['total_documents']:,}")
        logger.info(f"   Old format: {analysis['old_format_count']:,}")
        logger.info(f"   New format: {analysis['new_format_count']:,}")
        logger.info(f"   Corrupted: {len(analysis['corrupted_documents'])}")
        
        return analysis
    
    def _analyze_document_format(self, doc_data: Dict[str, Any]) -> str:
        """Determine if document is old or new format"""
        has_content = 'content' in doc_data and doc_data['content'] is not None
        has_chunk = 'chunk' in doc_data and doc_data['chunk'] is not None
        has_enhanced_metadata = all(field in doc_data for field in [
            'client_name', 'pm_initial', 'document_category'
        ])
        
        if has_chunk and has_enhanced_metadata:
            return "new_format"
        elif has_content and not has_chunk:
            return "old_format"
        else:
            return "unknown_format"
    
    def reprocess_document(self, blob_name: str) -> Dict[str, Any]:
        """Reprocess a single old-format document"""
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
            
            # Convert to new chunked format
            chunks = self._create_chunks_from_content(document_content, enhanced_metadata)
            
            # Store each chunk
            stored_chunks = []
            for i, chunk_content in enumerate(chunks):
                # Start with enhanced metadata but REMOVE the full content field
                chunk_data = {**enhanced_metadata}
                
                # Remove the old 'content' field - we only want 'chunk'
                chunk_data.pop('content', None)
                
                # Add chunk-specific fields
                chunk_data.update({
                    "chunk": chunk_content,
                    "chunk_id": f"{enhanced_metadata.get('document_id', blob_name.replace('.json', ''))}_{i}",
                    "chunk_index": i,
                    "parent_id": enhanced_metadata.get('document_id', blob_name.replace('.json', '')),
                    "processing_method": "reprocessed_enhanced"
                })
                
                # Store chunk
                chunk_blob_name = f"{chunk_data['chunk_id']}.json"
                self._store_chunk(chunk_blob_name, chunk_data)
                stored_chunks.append(chunk_blob_name)
            
            # Archive original document
            self._archive_original_document(blob_name, old_doc_data)
            
            return {
                "status": "success",
                "blob_name": blob_name,
                "chunks_created": len(stored_chunks),
                "stored_chunks": stored_chunks
            }
            
        except Exception as e:
            logger.error(f"Error reprocessing {blob_name}: {str(e)}")
            return {"status": "error", "blob_name": blob_name, "error": str(e)}
    
    def _create_chunks_from_content(self, content: str, metadata: Dict[str, Any]) -> List[str]:
        """Create chunks from content using consistent chunking logic"""
        chunk_size = 1000
        chunk_overlap = 200
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
                overlap_words = current_chunk.split()[-20:]  # Last 20 words for overlap
                current_chunk = " ".join(overlap_words) + " " + sentence
            else:
                current_chunk += sentence + ". " if current_chunk else sentence + ". "
        
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _store_chunk(self, blob_name: str, chunk_data: Dict[str, Any]):
        """Store chunk in jennifur-processed container"""
        blob_client = self.storage_client.get_blob_client(
            container='jennifur-processed',
            blob=blob_name
        )
        
        blob_client.upload_blob(
            json.dumps(chunk_data, indent=2),
            overwrite=True
        )
    
    def _archive_original_document(self, blob_name: str, doc_data: Dict[str, Any]):
        """Move original document to archived folder"""
        try:
            # Store in archived subfolder
            archive_blob_name = f"archived_originals/{blob_name}"
            archive_blob_client = self.storage_client.get_blob_client(
                container='jennifur-processed',
                blob=archive_blob_name
            )
            
            # Add archive metadata
            doc_data["archived_timestamp"] = datetime.utcnow().isoformat()
            doc_data["archive_reason"] = "reprocessed_with_enhanced_metadata"
            
            archive_blob_client.upload_blob(
                json.dumps(doc_data, indent=2),
                overwrite=True
            )
            
            # Delete original
            original_blob_client = self.storage_client.get_blob_client(
                container='jennifur-processed',
                blob=blob_name
            )
            original_blob_client.delete_blob()
            
            logger.debug(f"Archived {blob_name} to {archive_blob_name}")
            
        except Exception as e:
            logger.warning(f"Could not archive {blob_name}: {str(e)}")
    
    def reprocess_batch(self, old_format_documents: List[Dict[str, Any]], batch_size: int = 10) -> Dict[str, Any]:
        """Reprocess a batch of old-format documents"""
        logger.info(f"🔄 Starting batch reprocessing of {len(old_format_documents)} documents...")
        
        results = {
            "total_documents": len(old_format_documents),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "processing_results": [],
            "start_time": datetime.utcnow().isoformat()
        }
        
        for i, doc_info in enumerate(old_format_documents):
            blob_name = doc_info["blob_name"]
            
            logger.info(f"  Processing {i+1}/{len(old_format_documents)}: {doc_info['filename']}")
            
            result = self.reprocess_document(blob_name)
            results["processing_results"].append(result)
            
            if result["status"] == "success":
                results["successful"] += 1
            elif result["status"] == "skipped":
                results["skipped"] += 1
            else:
                results["failed"] += 1
            
            # Progress update
            if (i + 1) % batch_size == 0:
                logger.info(f"    Batch progress: {i+1}/{len(old_format_documents)} documents processed")
        
        results["end_time"] = datetime.utcnow().isoformat()
        
        logger.info(f"📊 Batch Reprocessing Complete:")
        logger.info(f"   Successful: {results['successful']:,}")
        logger.info(f"   Failed: {results['failed']:,}")
        logger.info(f"   Skipped: {results['skipped']:,}")
        
        return results

def main():
    """Main execution function"""
    logger.info("🚀 Starting Document Reprocessing Tool")
    
    reprocessor = DocumentReprocessor()
    
    # Step 1: Analyze all documents
    analysis = reprocessor.analyze_documents()
    
    if analysis["old_format_count"] == 0:
        logger.info("✅ No old-format documents found. All documents are already in new format!")
        return
    
    # Save analysis report
    with open('document_analysis_report.json', 'w') as f:
        json.dump(analysis, f, indent=2)
    logger.info(f"📄 Analysis report saved to document_analysis_report.json")
    
    # Ask user for confirmation
    print(f"\n🔍 ANALYSIS SUMMARY:")
    print(f"   📄 Total documents: {analysis['total_documents']:,}")
    print(f"   🔄 Need reprocessing: {analysis['old_format_count']:,}")
    print(f"   ✅ Already processed: {analysis['new_format_count']:,}")
    
    if analysis["old_format_count"] > 100:
        print(f"\n⚠️  WARNING: This will reprocess {analysis['old_format_count']:,} documents")
        print(f"   Estimated time: {analysis['old_format_count'] * 2 / 60:.1f} minutes")
    
    response = input("\n🤔 Proceed with reprocessing? (y/N): ").strip().lower()
    
    if response == 'y':
        # Step 2: Reprocess documents
        results = reprocessor.reprocess_batch(analysis["old_format_documents"])
        
        # Save results
        with open('reprocessing_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"📄 Results saved to reprocessing_results.json")
        
        print(f"\n🎉 REPROCESSING COMPLETE!")
        print(f"   ✅ Success: {results['successful']:,}")
        print(f"   ❌ Failed: {results['failed']:,}")
        print(f"   ⏭️ Skipped: {results['skipped']:,}")
        print(f"\n📋 Next steps:")
        print(f"   1. Update skillset to handle only 'chunk' field")
        print(f"   2. Reset and run indexer")
        print(f"   3. Verify search results")
        
    else:
        logger.info("❌ Reprocessing cancelled by user")

if __name__ == "__main__":
    main()