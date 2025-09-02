#!/usr/bin/env python3
"""
Inspect Azure Search Indexer Status Attributes
"""

import os
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def inspect_indexer():
    """Inspect indexer status object attributes"""
    
    # Initialize client
    indexer_client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    indexer_name = "jennifur-rag-indexer"
    
    try:
        # Get indexer status
        status = indexer_client.get_indexer_status(indexer_name)
        
        print("=== Status Object Attributes ===")
        print("Main status attributes:", dir(status))
        print("\nStatus value:", status.status)
        
        if status.last_result:
            print("\n=== Last Result Attributes ===")
            print("Last result attributes:", dir(status.last_result))
            
            # Try to access various attributes
            attrs_to_check = [
                'status', 'start_time', 'end_time', 
                'error_message', 'errors', 'warnings',
                'item_count', 'failed_item_count', 
                'initial_tracking_state', 'final_tracking_state'
            ]
            
            for attr in attrs_to_check:
                if hasattr(status.last_result, attr):
                    value = getattr(status.last_result, attr)
                    if value is not None:
                        if attr in ['errors', 'warnings'] and value:
                            print(f"\n{attr}: {len(value)} items")
                            # Show first item
                            first_item = value[0] if value else None
                            if first_item:
                                print(f"  First {attr[:-1]} attributes:", dir(first_item))
                        else:
                            print(f"{attr}: {value}")
        
        # Check execution history
        if status.execution_history:
            print("\n=== Execution History ===")
            print(f"Number of executions: {len(status.execution_history)}")
            if status.execution_history:
                first_exec = status.execution_history[0]
                print("First execution attributes:", dir(first_exec))
                
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    inspect_indexer()