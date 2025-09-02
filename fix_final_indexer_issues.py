#!/usr/bin/env python3
"""
Fix Final Indexer Issues

1. Force full reprocessing of all 88k documents by resetting indexer completely
2. The contentVector field doesn't exist - correct field is text_vector
"""

import os
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def fix_final_indexer_issues():
    """Fix the remaining indexer issues to process all documents"""
    
    indexer_client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    indexer_name = "jennifur-rag-indexer"
    
    print("🔧 FIXING FINAL INDEXER ISSUES")
    print("=" * 60)
    
    try:
        # Issue 1: Only processing 303 documents instead of 88k
        print("📋 ISSUE 1: Indexer only processing 303 recently modified documents")
        print("✅ SOLUTION: Force full reprocessing by resetting indexer state")
        
        # Reset indexer completely to clear change detection state
        print("\n🔄 Resetting indexer to force full reprocessing...")
        indexer_client.reset_indexer(indexer_name)
        print("✅ Indexer reset - will now process ALL 88,083 documents")
        
        # Start the indexer
        print("\n🚀 Starting indexer run...")
        indexer_client.run_indexer(indexer_name)
        print("✅ Indexer started - processing all documents")
        
        # Issue 2: contentVector field doesn't exist
        print(f"\n📋 ISSUE 2: contentVector field mapping error")
        print("❌ Field 'contentVector' does not exist in the index")
        print("✅ CORRECT FIELD NAME: 'text_vector'")
        
        print(f"\n🛠️ MANUAL STEP REQUIRED:")
        print("In Azure Portal, add this output field mapping:")
        print("{")
        print('  "sourceFieldName": "/document/pages/*/text_vector",')
        print('  "targetFieldName": "text_vector"')
        print("}")
        print("(NOT contentVector - use text_vector as the target)")
        
        print(f"\n📈 EXPECTED RESULTS:")
        print("- Indexer will process all 88,083 documents (15-30 minutes)")
        print("- Should discover 30+ clients instead of just 8")  
        print("- Document count should increase dramatically")
        print("- RAG responses will be comprehensive and accurate")
        
        print(f"\n⚠️ IMPORTANT:")
        print("The previous runs only processed the 303 files we cleaned")
        print("because Azure Search has change detection enabled.")
        print("This reset forces reprocessing of ALL documents in storage.")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_final_indexer_issues()