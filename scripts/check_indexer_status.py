#!/usr/bin/env python3
"""
Quick Indexer Status Check
"""

import os
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

def check_status():
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
    
    credential = AzureKeyCredential(admin_key)
    indexer_client = SearchIndexerClient(endpoint=endpoint, credential=credential)
    search_client = SearchClient(endpoint=endpoint, index_name="jennifur-rag", credential=credential)
    
    # Check indexer status
    try:
        status = indexer_client.get_indexer_status("jennifur-rag-indexer")
        print(f"Indexer Status: {str(status.status)}")
        if status.last_result:
            print(f"Last Result: {str(status.last_result.status)}")
            print(f"Start Time: {status.last_result.start_time}")
            print(f"End Time: {status.last_result.end_time}")
            if hasattr(status.last_result, 'item_count'):
                print(f"Items Processed: {status.last_result.item_count}")
    except Exception as e:
        print(f"Error: {str(e)}")
    
    # Check document count
    try:
        results = search_client.search("*", include_total_count=True, top=1)
        count = results.get_count()
        print(f"Current Document Count: {count:,}")
    except Exception as e:
        print(f"Document Count Error: {str(e)}")

if __name__ == "__main__":
    check_status()