"""
Enhanced Document Processor with Client Metadata
Processes new documents and automatically adds client metadata during indexing
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import asdict

from .client_metadata_extractor import ClientMetadataExtractor, ClientInfo

class EnhancedDocumentProcessor:
    """Process documents with automatic client metadata extraction"""
    
    def __init__(self):
        self.metadata_extractor = ClientMetadataExtractor()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging for the document processor"""
        logger = logging.getLogger('document_processor')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def process_document_for_indexing(
        self, 
        document_content: str,
        document_path: str,
        filename: str,
        additional_metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Process a single document and extract all metadata for indexing
        
        Args:
            document_content: The text content of the document
            document_path: Full SharePoint path to the document
            filename: Name of the file
            additional_metadata: Any additional metadata to include
            
        Returns:
            Dictionary with all metadata ready for Azure Search indexing
        """
        try:
            # Extract client information
            client_info = self.metadata_extractor.extract_client_info(document_path)
            
            # Create base metadata
            processing_timestamp = datetime.utcnow().isoformat() + "Z"
            
            metadata = {
                # Original fields
                "document_path": document_path,
                "filename": filename,
                "processed_timestamp": processing_timestamp,
                "content": document_content,
                "content_length": len(document_content),
                
                # Client metadata
                "client_name": client_info.client_name,
                "pm_initial": client_info.pm_initial,
                "pm_name": client_info.pm_name,
                "document_category": client_info.document_category,
                "is_client_specific": client_info.is_client_specific,
                "metadata_updated_timestamp": processing_timestamp,
                
                # Additional computed metadata
                "folder_depth": len([p for p in document_path.split('/') if p]),
                "file_extension": os.path.splitext(filename)[1].lower(),
                "has_client_folder": client_info.is_client_specific,
                
                # Document analysis
                "word_count": len(document_content.split()) if document_content else 0,
                "character_count": len(document_content) if document_content else 0,
            }
            
            # Add any additional metadata
            if additional_metadata:
                metadata.update(additional_metadata)
            
            # Log processing
            self.logger.info(f"Processed document: {filename}")
            self.logger.info(f"  Client: {client_info.client_name}")
            self.logger.info(f"  Category: {client_info.document_category}")
            self.logger.info(f"  PM: {client_info.pm_initial}")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error processing document {filename}: {str(e)}")
            # Return minimal metadata on error
            return {
                "document_path": document_path,
                "filename": filename,
                "processed_timestamp": datetime.utcnow().isoformat() + "Z",
                "content": document_content,
                "client_name": "Processing Error",
                "pm_initial": "N/A",
                "document_category": "error",
                "is_client_specific": False,
                "error": str(e)
            }
    
    def process_document_batch(
        self,
        documents: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Process a batch of documents for indexing
        
        Args:
            documents: List of documents with keys: content, document_path, filename
            
        Returns:
            List of processed documents with metadata
        """
        processed_documents = []
        
        self.logger.info(f"Processing batch of {len(documents)} documents...")
        
        for i, doc in enumerate(documents, 1):
            try:
                processed_doc = self.process_document_for_indexing(
                    document_content=doc.get('content', ''),
                    document_path=doc.get('document_path', ''),
                    filename=doc.get('filename', ''),
                    additional_metadata=doc.get('metadata', {})
                )
                
                processed_documents.append(processed_doc)
                
                # Progress logging
                if i % 10 == 0 or i == len(documents):
                    self.logger.info(f"  Processed {i}/{len(documents)} documents")
                
            except Exception as e:
                self.logger.error(f"Error in batch processing document {i}: {str(e)}")
                continue
        
        self.logger.info(f"Batch processing completed: {len(processed_documents)}/{len(documents)} successful")
        return processed_documents
    
    def create_search_document(
        self,
        content: str,
        document_path: str,
        filename: str,
        chunk_id: str,
        parent_id: str,
        text_vector: List[float],
        additional_fields: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Create a complete Azure Search document with client metadata
        
        Args:
            content: Document text content
            document_path: Full path to document
            filename: Document filename
            chunk_id: Unique chunk identifier
            parent_id: Parent document identifier
            text_vector: Embedding vector for the content
            additional_fields: Any additional fields to include
            
        Returns:
            Complete document ready for Azure Search indexing
        """
        # Process the document to get metadata
        processed_doc = self.process_document_for_indexing(
            document_content=content,
            document_path=document_path,
            filename=filename
        )
        
        # Create the search document
        search_doc = {
            "chunk_id": chunk_id,
            "parent_id": parent_id,
            "chunk": content,
            "text_vector": text_vector,
            "document_path": document_path,
            "filename": filename,
            "title": filename,  # Could be enhanced with actual document title
            "status": "ready_for_rag",
            
            # Client metadata
            "client_name": processed_doc["client_name"],
            "pm_initial": processed_doc["pm_initial"],
            "pm_name": processed_doc["pm_name"],
            "document_category": processed_doc["document_category"],
            "is_client_specific": processed_doc["is_client_specific"],
            "metadata_updated_timestamp": processed_doc["metadata_updated_timestamp"],
            
            # Additional computed fields
            "content_length": processed_doc["content_length"],
            "word_count": processed_doc["word_count"],
            "file_extension": processed_doc["file_extension"],
            "folder_depth": processed_doc["folder_depth"],
            "processed_timestamp": processed_doc["processed_timestamp"]
        }
        
        # Add any additional fields
        if additional_fields:
            search_doc.update(additional_fields)
        
        return search_doc
    
    def validate_document_metadata(self, document: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate that a document has all required metadata fields
        
        Args:
            document: Document to validate
            
        Returns:
            Tuple of (is_valid, list_of_missing_fields)
        """
        required_fields = {
            'chunk_id', 'document_path', 'filename', 'chunk',
            'client_name', 'pm_initial', 'document_category', 
            'is_client_specific', 'metadata_updated_timestamp'
        }
        
        missing_fields = []
        for field in required_fields:
            if field not in document or document[field] is None:
                missing_fields.append(field)
        
        is_valid = len(missing_fields) == 0
        return is_valid, missing_fields
    
    def get_processing_statistics(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate statistics about processed documents
        
        Args:
            documents: List of processed documents
            
        Returns:
            Dictionary with processing statistics
        """
        if not documents:
            return {"total": 0}
        
        # Extract client info for statistics
        client_infos = []
        for doc in documents:
            from .client_metadata_extractor import ClientInfo
            client_info = ClientInfo(
                client_name=doc.get('client_name', 'Unknown'),
                pm_initial=doc.get('pm_initial', 'N/A'),
                pm_name=doc.get('pm_name', 'N/A'),
                folder_path=doc.get('document_path', ''),
                is_client_specific=doc.get('is_client_specific', False),
                document_category=doc.get('document_category', 'general')
            )
            client_infos.append(client_info)
        
        # Get base statistics
        stats = self.metadata_extractor.get_client_statistics(client_infos)
        
        # Add processing-specific stats
        stats.update({
            "processing_timestamp": datetime.utcnow().isoformat(),
            "average_content_length": sum(doc.get('content_length', 0) for doc in documents) / len(documents),
            "total_content_length": sum(doc.get('content_length', 0) for doc in documents),
            "file_types": {}
        })
        
        # File type statistics
        for doc in documents:
            ext = doc.get('file_extension', 'unknown')
            if ext in stats['file_types']:
                stats['file_types'][ext] += 1
            else:
                stats['file_types'][ext] = 1
        
        return stats

# Utility functions for integration
def process_sharepoint_document(
    content: str,
    sharepoint_path: str,
    filename: str
) -> Dict[str, Any]:
    """
    Utility function to process a single SharePoint document
    
    Args:
        content: Document text content
        sharepoint_path: SharePoint path (e.g., "/ClientName (PM-X)/folder/file.pdf")
        filename: Document filename
        
    Returns:
        Processed document with client metadata
    """
    processor = EnhancedDocumentProcessor()
    return processor.process_document_for_indexing(
        document_content=content,
        document_path=sharepoint_path,
        filename=filename
    )

# Test function
def test_document_processor():
    """Test the enhanced document processor"""
    processor = EnhancedDocumentProcessor()
    
    test_documents = [
        {
            "content": "This is a financial report for Q1 2024...",
            "document_path": "/Camelot (PM-C)/_08. Financials/Q1_2024_Report.pdf",
            "filename": "Q1_2024_Report.pdf"
        },
        {
            "content": "Employee handbook policies and procedures...",
            "document_path": "/Autobahn Tools/Training/Employee_Handbook.pdf", 
            "filename": "Employee_Handbook.pdf"
        },
        {
            "content": "Meeting notes from client discussion...",
            "document_path": "/Phoenix Corporation (PM-S)/Meetings/weekly_standup.docx",
            "filename": "weekly_standup.docx"
        }
    ]
    
    print("🧪 Testing Enhanced Document Processor")
    print("=" * 50)
    
    # Process batch
    processed_docs = processor.process_document_batch(test_documents)
    
    # Generate statistics
    stats = processor.get_processing_statistics(processed_docs)
    
    print(f"\n📊 Processing Statistics:")
    print(f"   Total documents: {stats['total_documents']}")
    print(f"   Client-specific: {stats['client_specific']}")
    print(f"   Clients found: {len(stats['clients'])}")
    
    for client, count in stats['clients'].items():
        print(f"     - {client}: {count}")

if __name__ == "__main__":
    test_document_processor()