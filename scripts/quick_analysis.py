#!/usr/bin/env python3
"""
Quick Document Analysis - Sample first 500 documents to understand distribution
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def quick_analysis():
    """Quick analysis of first 500 documents"""
    storage_client = BlobServiceClient.from_connection_string(
        os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    )
    container_client = storage_client.get_container_client('jennifur-processed')
    
    analysis = {
        "sample_size": 500,
        "total_analyzed": 0,
        "old_format_count": 0,
        "new_format_count": 0,
        "corrupted_count": 0,
        "old_format_examples": [],
        "new_format_examples": [],
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.info(f"🔍 Quick analysis of first {analysis['sample_size']} documents...")
    
    blob_count = 0
    for blob in container_client.list_blobs():
        if blob_count >= analysis['sample_size']:
            break
            
        blob_count += 1
        analysis["total_analyzed"] += 1
        
        try:
            blob_client = storage_client.get_blob_client(
                container='jennifur-processed', 
                blob=blob.name
            )
            content = blob_client.download_blob().readall().decode('utf-8')
            doc_data = json.loads(content)
            
            # Check format
            has_content = 'content' in doc_data and doc_data['content'] is not None
            has_chunk = 'chunk' in doc_data and doc_data['chunk'] is not None
            has_enhanced_metadata = all(field in doc_data for field in [
                'client_name', 'pm_initial', 'document_category'
            ])
            
            if has_chunk and has_enhanced_metadata:
                analysis["new_format_count"] += 1
                if len(analysis["new_format_examples"]) < 5:
                    analysis["new_format_examples"].append({
                        "blob_name": blob.name,
                        "filename": doc_data.get("filename", "unknown"),
                        "client_name": doc_data.get("client_name", "unknown"),
                        "chunk_id": doc_data.get("chunk_id", "unknown")
                    })
            elif has_content and not has_chunk:
                analysis["old_format_count"] += 1
                if len(analysis["old_format_examples"]) < 5:
                    analysis["old_format_examples"].append({
                        "blob_name": blob.name,
                        "filename": doc_data.get("filename", "unknown"),
                        "document_path": doc_data.get("document_path", "unknown"),
                        "last_modified": blob.last_modified.isoformat()
                    })
            
            if blob_count % 50 == 0:
                logger.info(f"  Analyzed {blob_count}/500 documents...")
                
        except Exception as e:
            analysis["corrupted_count"] += 1
            logger.warning(f"Error analyzing {blob.name}: {str(e)}")
    
    # Summary
    logger.info(f"📊 Quick Analysis Results:")
    logger.info(f"   Sample size: {analysis['total_analyzed']} documents")
    logger.info(f"   Old format: {analysis['old_format_count']} ({analysis['old_format_count']/analysis['total_analyzed']*100:.1f}%)")
    logger.info(f"   New format: {analysis['new_format_count']} ({analysis['new_format_count']/analysis['total_analyzed']*100:.1f}%)")
    logger.info(f"   Corrupted: {analysis['corrupted_count']}")
    
    # Extrapolate to full dataset
    if analysis["total_analyzed"] > 0:
        # Estimate total documents (rough estimate)
        estimated_total = 15000  # From previous runs
        old_percentage = analysis['old_format_count'] / analysis['total_analyzed']
        estimated_old_docs = int(estimated_total * old_percentage)
        
        logger.info(f"\n📈 Extrapolated Estimates (assuming ~15k total docs):")
        logger.info(f"   Estimated old format documents: {estimated_old_docs:,}")
        logger.info(f"   Estimated processing time: {estimated_old_docs * 2 / 60:.1f} minutes")
    
    # Save results
    with open('quick_analysis_results.json', 'w') as f:
        json.dump(analysis, f, indent=2)
    
    logger.info(f"📄 Results saved to quick_analysis_results.json")
    
    return analysis

if __name__ == "__main__":
    quick_analysis()