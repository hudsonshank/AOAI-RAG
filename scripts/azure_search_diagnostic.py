#!/usr/bin/env python3
"""
Azure Search Index Diagnostic Script
Analyzes the search index to understand document count discrepancies
"""

import os
import json
from dotenv import load_dotenv
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class SearchIndexDiagnostic:
    def __init__(self):
        self.search_client = SearchClient(
            endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
            index_name=os.getenv("EXISTING_INDEX_NAME"),
            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
        )
        
    def analyze_index(self):
        """Comprehensive index analysis"""
        print("🔍 AZURE SEARCH INDEX DIAGNOSTIC")
        print("=" * 60)
        
        # Basic index info
        index_name = os.getenv("EXISTING_INDEX_NAME")
        print(f"Index Name: {index_name}")
        print(f"Endpoint: {os.getenv('AZURE_SEARCH_ENDPOINT')}")
        
        # 1. Total document count
        print(f"\n1. TOTAL DOCUMENT COUNT")
        print("-" * 30)
        
        try:
            # Search for all documents
            all_docs = self.search_client.search(
                search_text="*",
                include_total_count=True,
                top=1  # We just want the count
            )
            
            total_count = all_docs.get_count()
            print(f"Total documents in index: {total_count:,}")
            
        except Exception as e:
            print(f"Error getting total count: {str(e)}")
            return
        
        # 2. Document sampling
        print(f"\n2. DOCUMENT SAMPLING")
        print("-" * 30)
        
        try:
            # Get a sample of documents
            sample_docs = self.search_client.search(
                search_text="*",
                top=50,  # Sample size
                select=["filename", "document_path", "title", "chunk_id"] if "chunk_id" in ["filename"] else ["*"]
            )
            
            sample_list = list(sample_docs)
            print(f"Sample retrieved: {len(sample_list)} documents")
            
            # Analyze document structure
            if sample_list:
                first_doc = sample_list[0]
                print(f"\nDocument structure (first document):")
                for key, value in first_doc.items():
                    if isinstance(value, str) and len(value) > 100:
                        print(f"  {key}: {value[:100]}...")
                    else:
                        print(f"  {key}: {value}")
                
                # Check for common fields
                print(f"\nCommon fields across sample:")
                all_fields = set()
                for doc in sample_list[:10]:  # Check first 10
                    all_fields.update(doc.keys())
                
                for field in sorted(all_fields):
                    print(f"  - {field}")
                    
        except Exception as e:
            print(f"Error sampling documents: {str(e)}")
        
        # 3. Chunk analysis (if chunked)
        print(f"\n3. DOCUMENT CHUNKING ANALYSIS")
        print("-" * 30)
        
        try:
            # Check if documents are chunked
            chunk_search = self.search_client.search(
                search_text="*",
                top=100,
                select=["filename", "chunk_id", "title"] if "chunk_id" in ["filename"] else ["filename", "document_path"]
            )
            
            chunk_list = list(chunk_search)
            
            # Count unique files vs total chunks
            filenames = set()
            total_chunks = 0
            
            for doc in chunk_list:
                filename = doc.get('filename', doc.get('sourcefile', 'unknown'))
                filenames.add(filename)
                total_chunks += 1
            
            print(f"Unique files in sample: {len(filenames)}")
            print(f"Total chunks in sample: {total_chunks}")
            print(f"Average chunks per file: {total_chunks/len(filenames) if filenames else 0:.1f}")
            
            # Show some example filenames
            print(f"\nExample filenames:")
            for i, filename in enumerate(sorted(filenames)[:10]):
                print(f"  {i+1}. {filename}")
            
        except Exception as e:
            print(f"Error analyzing chunks: {str(e)}")
        
        # 4. Search behavior analysis
        print(f"\n4. SEARCH BEHAVIOR ANALYSIS")
        print("-" * 30)
        
        test_queries = [
            "remote work policy",
            "expense",
            "vacation",
            "employee handbook",
            "*"
        ]
        
        for query in test_queries:
            try:
                results = self.search_client.search(
                    search_text=query,
                    top=5,
                    include_total_count=True
                )
                
                result_list = list(results)
                count = results.get_count()
                
                print(f"\nQuery: '{query}'")
                print(f"  Total matches: {count:,}")
                print(f"  Retrieved: {len(result_list)}")
                
                if result_list:
                    print(f"  Top result score: {result_list[0].get('@search.score', 'N/A')}")
                    print(f"  Top result file: {result_list[0].get('filename', result_list[0].get('sourcefile', 'Unknown'))}")
                    
            except Exception as e:
                print(f"  Error with query '{query}': {str(e)}")
        
        # 5. Investigate potential filtering
        print(f"\n5. POTENTIAL FILTERING ISSUES")
        print("-" * 30)
        
        # Check if there are any implicit filters in your search calls
        print("Checking for potential issues:")
        
        # Test different search modes
        search_modes = ["any", "all"]
        for mode in search_modes:
            try:
                results = self.search_client.search(
                    search_text="*",
                    search_mode=mode,
                    top=10,
                    include_total_count=True
                )
                
                count = results.get_count()
                print(f"  Search mode '{mode}': {count:,} total documents")
                
            except Exception as e:
                print(f"  Error with search mode '{mode}': {str(e)}")
        
        # 6. Raw API investigation
        print(f"\n6. RECOMMENDATIONS")
        print("-" * 30)
        
        if total_count < 1000:
            print("⚠️  Issue detected: Low document count suggests potential problems:")
            print("   1. Index may not be fully populated")
            print("   2. Documents may have failed processing")
            print("   3. There may be indexing errors")
            print("   4. Different index may be in use")
        
        print(f"\n💡 Next steps:")
        print(f"   1. Check Azure Portal for index '{index_name}'")
        print(f"   2. Verify indexer status and error logs")
        print(f"   3. Check if documents failed to process")
        print(f"   4. Validate index name in .env file")
        print(f"   5. Look for indexing warnings/errors")

    def check_index_schema(self):
        """Check the index schema"""
        print(f"\n7. INDEX SCHEMA ANALYSIS")
        print("-" * 30)
        
        try:
            # This would require admin client to get schema
            print("Note: Schema analysis requires additional permissions")
            print("Check Azure Portal -> Search Service -> Indexes -> jennifur-rag")
            
        except Exception as e:
            print(f"Could not retrieve schema: {str(e)}")

def main():
    """Run the diagnostic"""
    try:
        diagnostic = SearchIndexDiagnostic()
        diagnostic.analyze_index()
        diagnostic.check_index_schema()
        
        print(f"\n" + "=" * 60)
        print("DIAGNOSTIC COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"Diagnostic failed: {str(e)}")
        print("Check your environment variables and Azure credentials")

if __name__ == "__main__":
    main()