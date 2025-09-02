#!/usr/bin/env python3
"""
Monitor Azure Search Indexer Progress

This script monitors the indexer progress as it processes the ~88k documents
and reports on client coverage and document counts.
"""

import os
import time
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

def monitor_indexer_progress():
    """Monitor indexer progress and document/client counts"""
    
    indexer_client = SearchIndexerClient(
        endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
        credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
    )
    
    print("📊 Monitoring Indexer Progress")
    print("=" * 60)
    print("Expected: 88,083 documents across 30+ clients")
    print("Current target: Process as many valid documents as possible")
    print("-" * 60)
    
    while True:
        try:
            status = indexer_client.get_indexer_status("jennifur-rag-indexer")
            
            if status.last_result:
                current_time = time.strftime("%H:%M:%S")
                
                total_items = status.last_result.item_count or 0
                failed_items = status.last_result.failed_item_count or 0
                processed_items = total_items - failed_items
                
                print(f"\n[{current_time}] Indexer Status: {status.last_result.status}")
                print(f"Total Items Found: {total_items:,}")
                print(f"Successfully Processed: {processed_items:,}")
                print(f"Failed Items: {failed_items:,}")
                
                if total_items > 0:
                    success_rate = (processed_items / total_items) * 100
                    progress_pct = (total_items / 88083) * 100
                    
                    print(f"Success Rate: {success_rate:.1f}%")
                    print(f"Discovery Progress: {progress_pct:.1f}% of expected documents")
                    
                    # Estimate remaining time if in progress
                    if status.last_result.status == "inProgress" and status.last_result.start_time:
                        start_time = status.last_result.start_time
                        current_time_obj = status.last_result.end_time or start_time
                        elapsed_seconds = (current_time_obj - start_time).total_seconds()
                        
                        if elapsed_seconds > 60 and total_items > 100:
                            rate = total_items / elapsed_seconds
                            remaining_items = 88083 - total_items
                            eta_seconds = remaining_items / rate if rate > 0 else 0
                            
                            print(f"Processing Rate: {rate:.1f} docs/second")
                            print(f"ETA for completion: {eta_seconds/60:.0f} minutes")
                
                # Check if indexer has completed or failed
                if status.last_result.status in ["success", "transientFailure", "error"]:
                    print(f"\n🏁 INDEXER COMPLETED with status: {status.last_result.status}")
                    
                    if processed_items > 10000:  # Good result
                        print(f"✅ SUCCESS: Indexed {processed_items:,} documents!")
                        print(f"📊 Now test RAG system to verify client coverage")
                    elif processed_items > 1000:  # Partial success
                        print(f"⚠️ PARTIAL SUCCESS: Indexed {processed_items:,} documents")
                        print(f"🔧 May need to address remaining {failed_items:,} failed items")
                    else:
                        print(f"❌ POOR RESULT: Only {processed_items:,} documents indexed")
                        print(f"🚨 Major issues still need to be resolved")
                    
                    break
            else:
                print(f"[{time.strftime('%H:%M:%S')}] Indexer status: {status.status}")
                print("No execution results available yet")
        
        except Exception as e:
            print(f"Error checking status: {str(e)}")
        
        # Wait before next check
        time.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    monitor_indexer_progress()