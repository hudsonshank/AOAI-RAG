#!/usr/bin/env python3
"""
Analyze Document Structure in Blob Storage
Check what fields are actually available in the source documents
"""

import os
import json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

class DocumentStructureAnalyzer:
    """Analyze the actual structure of documents in blob storage"""
    
    def __init__(self):
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = os.getenv("AZURE_STORAGE_CONTAINER", "jennifur-processed")
        
        if not self.connection_string:
            raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
    
    def analyze_sample_documents(self, sample_size: int = 10) -> dict:
        """Analyze a sample of documents to understand their structure"""
        print(f"🔍 Analyzing {sample_size} sample documents from container '{self.container_name}'...")
        
        blob_list = list(self.container_client.list_blobs())[:sample_size]
        
        if not blob_list:
            print("❌ No processed documents found in container")
            return {}
        
        field_analysis = {
            "total_analyzed": 0,
            "fields_found": {},
            "sample_documents": []
        }
        
        for i, blob in enumerate(blob_list):
            try:
                print(f"   Analyzing document {i+1}/{len(blob_list)}: {blob.name}")
                
                # Download and parse the blob
                blob_client = self.container_client.get_blob_client(blob.name)
                content = blob_client.download_blob().readall().decode('utf-8')
                
                # Parse JSON
                doc_data = json.loads(content)
                field_analysis["total_analyzed"] += 1
                
                # Track all fields found
                for field in doc_data.keys():
                    if field in field_analysis["fields_found"]:
                        field_analysis["fields_found"][field] += 1
                    else:
                        field_analysis["fields_found"][field] = 1
                
                # Keep sample for detailed analysis
                if len(field_analysis["sample_documents"]) < 3:
                    sample_doc = {}
                    for key, value in doc_data.items():
                        if isinstance(value, str) and len(value) > 100:
                            sample_doc[key] = value[:100] + "..."
                        else:
                            sample_doc[key] = value
                    field_analysis["sample_documents"].append({
                        "filename": blob.name,
                        "fields": sample_doc
                    })
                
            except Exception as e:
                print(f"   ❌ Error analyzing {blob.name}: {str(e)}")
        
        return field_analysis
    
    def check_chunk_vs_content_fields(self) -> dict:
        """Specifically check for 'chunk' vs 'content' field usage"""
        print(f"🔍 Checking 'chunk' vs 'content' field usage...")
        
        blob_list = list(self.container_client.list_blobs())[:50]  # Check more documents
        
        results = {
            "total_checked": 0,
            "has_chunk_field": 0,
            "has_content_field": 0, 
            "has_both": 0,
            "has_neither": 0,
            "empty_chunk": 0,
            "empty_content": 0,
            "examples": []
        }
        
        for blob in blob_list:
            try:
                blob_client = self.container_client.get_blob_client(blob.name)
                content = blob_client.download_blob().readall().decode('utf-8')
                doc_data = json.loads(content)
                
                results["total_checked"] += 1
                
                has_chunk = "chunk" in doc_data
                has_content = "content" in doc_data
                
                if has_chunk:
                    results["has_chunk_field"] += 1
                    if not doc_data["chunk"] or doc_data["chunk"].strip() == "":
                        results["empty_chunk"] += 1
                
                if has_content:
                    results["has_content_field"] += 1
                    if not doc_data["content"] or doc_data["content"].strip() == "":
                        results["empty_content"] += 1
                
                if has_chunk and has_content:
                    results["has_both"] += 1
                elif not has_chunk and not has_content:
                    results["has_neither"] += 1
                
                # Collect examples
                if len(results["examples"]) < 5:
                    results["examples"].append({
                        "filename": blob.name,
                        "has_chunk": has_chunk,
                        "has_content": has_content,
                        "chunk_empty": has_chunk and (not doc_data["chunk"] or doc_data["chunk"].strip() == ""),
                        "content_empty": has_content and (not doc_data["content"] or doc_data["content"].strip() == "")
                    })
                
            except Exception as e:
                print(f"   ❌ Error checking {blob.name}: {str(e)}")
        
        return results

def main():
    """Main execution function"""
    print("🔍 DOCUMENT STRUCTURE ANALYSIS")
    print("=" * 60)
    print("Analyzing documents in blob storage to understand field structure")
    print()
    
    try:
        analyzer = DocumentStructureAnalyzer()
        
        # General structure analysis
        print("📊 Step 1: General document structure analysis")
        structure_analysis = analyzer.analyze_sample_documents(10)
        
        if structure_analysis:
            print(f"\n📋 Field Analysis (from {structure_analysis['total_analyzed']} documents):")
            for field, count in sorted(structure_analysis["fields_found"].items(), key=lambda x: x[1], reverse=True):
                percentage = (count / structure_analysis['total_analyzed']) * 100
                print(f"   {field}: {count}/{structure_analysis['total_analyzed']} ({percentage:.1f}%)")
            
            print(f"\n📄 Sample Document Structures:")
            for i, sample in enumerate(structure_analysis["sample_documents"], 1):
                print(f"\n   Sample {i} ({sample['filename']}):")
                for field, value in sample["fields"].items():
                    print(f"     {field}: {value}")
        
        print(f"\n" + "="*60)
        
        # Chunk vs Content specific analysis
        print("📊 Step 2: 'chunk' vs 'content' field analysis")
        chunk_analysis = analyzer.check_chunk_vs_content_fields()
        
        print(f"\n📋 Field Usage Analysis (from {chunk_analysis['total_checked']} documents):")
        print(f"   Documents with 'chunk' field: {chunk_analysis['has_chunk_field']}")
        print(f"   Documents with 'content' field: {chunk_analysis['has_content_field']}")
        print(f"   Documents with both fields: {chunk_analysis['has_both']}")
        print(f"   Documents with neither field: {chunk_analysis['has_neither']}")
        print(f"   Documents with empty 'chunk': {chunk_analysis['empty_chunk']}")
        print(f"   Documents with empty 'content': {chunk_analysis['empty_content']}")
        
        print(f"\n📄 Examples:")
        for example in chunk_analysis["examples"]:
            print(f"   {example['filename']}:")
            print(f"     Has chunk: {example['has_chunk']} (empty: {example['chunk_empty']})")
            print(f"     Has content: {example['has_content']} (empty: {example['content_empty']})")
        
        print(f"\n💡 Analysis Summary:")
        if chunk_analysis['has_neither'] > 0:
            print("   ⚠️  Some documents have neither 'chunk' nor 'content' fields")
        if chunk_analysis['empty_chunk'] > 0:
            print(f"   ⚠️  {chunk_analysis['empty_chunk']} documents have empty 'chunk' fields")
        if chunk_analysis['has_both'] > 0:
            print(f"   ℹ️  {chunk_analysis['has_both']} documents have both 'chunk' and 'content' fields")
        
        print("\n🔧 Recommendations:")
        if chunk_analysis['empty_chunk'] > 0 or chunk_analysis['has_neither'] > 0:
            print("   1. Update skillset to handle missing/empty 'chunk' fields")
            print("   2. Consider fallback to 'content' field if 'chunk' is empty")
            print("   3. Add conditional logic to skillset input mapping")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()