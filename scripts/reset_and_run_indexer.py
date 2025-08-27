#!/usr/bin/env python3
"""
Reset and Run Azure Search Indexer with Clean Data
Resets the indexer and runs it with the newly standardized documents
"""

import os
import time
from datetime import datetime
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class IndexerManager:
    """Manage Azure Search indexer reset and execution"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.index_name = os.getenv("EXISTING_INDEX_NAME", "jennifur-rag")
        self.indexer_name = "jennifur-rag-indexer"  # Use the actual existing indexer
        self.skillset_name = os.getenv("AZURE_SEARCH_SKILLSET_NAME", "jennifur-skillset")
        
        if not all([self.endpoint, self.admin_key]):
            raise ValueError("Missing required Azure Search configuration")
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.indexer_client = SearchIndexerClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
        self.search_client = SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=self.credential
        )
    
    def get_indexer_status(self) -> dict:
        """Get current indexer status"""
        try:
            indexer = self.indexer_client.get_indexer(self.indexer_name)
            status = self.indexer_client.get_indexer_status(self.indexer_name)
            
            return {
                "name": indexer.name,
                "description": indexer.description,
                "status": str(status.status) if status.status else "unknown",
                "last_result": str(status.last_result.status) if status.last_result and status.last_result.status else "none",
                "execution_history": len(status.execution_history) if status.execution_history else 0
            }
        except Exception as e:
            return {"error": str(e)}
    
    def reset_indexer(self) -> bool:
        """Reset the indexer to clear all processed state"""
        try:
            print(f"🔄 Resetting indexer '{self.indexer_name}'...")
            
            # Reset the indexer
            self.indexer_client.reset_indexer(self.indexer_name)
            print("✅ Indexer reset successfully")
            
            # Wait a moment for the reset to take effect
            print("⏳ Waiting 10 seconds for reset to propagate...")
            time.sleep(10)
            
            return True
            
        except Exception as e:
            print(f"❌ Error resetting indexer: {str(e)}")
            return False
    
    def run_indexer(self) -> bool:
        """Run the indexer to process documents"""
        try:
            print(f"▶️  Running indexer '{self.indexer_name}'...")
            
            # Start the indexer
            self.indexer_client.run_indexer(self.indexer_name)
            print("✅ Indexer started successfully")
            
            return True
            
        except Exception as e:
            print(f"❌ Error running indexer: {str(e)}")
            return False
    
    def monitor_indexer_progress(self, max_wait_minutes: int = 30) -> dict:
        """Monitor indexer progress"""
        print(f"📊 Monitoring indexer progress (max {max_wait_minutes} minutes)...")
        
        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60
        
        while (time.time() - start_time) < max_wait_seconds:
            try:
                status = self.indexer_client.get_indexer_status(self.indexer_name)
                current_status = str(status.status) if status.status else "unknown"
                
                print(f"   Status: {current_status}")
                
                if status.last_result:
                    result_status = str(status.last_result.status) if status.last_result.status else "unknown"
                    print(f"   Last result: {result_status}")
                    
                    if result_status.lower() in ["success", "transientfailure", "reset"]:
                        # Check for completion details
                        if hasattr(status.last_result, 'item_count') and status.last_result.item_count is not None:
                            print(f"   Items processed: {status.last_result.item_count}")
                        if hasattr(status.last_result, 'failed_item_count') and status.last_result.failed_item_count is not None:
                            print(f"   Failed items: {status.last_result.failed_item_count}")
                        if hasattr(status.last_result, 'warning_count') and status.last_result.warning_count is not None:
                            print(f"   Warnings: {status.last_result.warning_count}")
                        
                        # If completed, return results
                        if result_status.lower() == "success":
                            return {
                                "completed": True,
                                "status": current_status,
                                "items_processed": getattr(status.last_result, 'item_count', 0),
                                "failed_items": getattr(status.last_result, 'failed_item_count', 0),
                                "warnings": getattr(status.last_result, 'warning_count', 0)
                            }
                        elif result_status.lower() == "transientfailure":
                            print(f"⚠️  Indexer completed with transient failures")
                            return {
                                "completed": True,
                                "status": current_status,
                                "items_processed": getattr(status.last_result, 'item_count', 0),
                                "failed_items": getattr(status.last_result, 'failed_item_count', 0),
                                "warnings": getattr(status.last_result, 'warning_count', 0),
                                "has_failures": True
                            }
                
                # If still running, wait and check again
                if current_status.lower() == "running":
                    print("⏳ Still running, checking again in 30 seconds...")
                    time.sleep(30)
                else:
                    print(f"⏳ Waiting for execution (status: {current_status})...")
                    time.sleep(10)
                    
            except Exception as e:
                print(f"❌ Error monitoring indexer: {str(e)}")
                time.sleep(10)
        
        # Timeout reached
        return {
            "completed": False,
            "timeout": True,
            "message": f"Timeout after {max_wait_minutes} minutes"
        }
    
    def get_document_count(self) -> int:
        """Get current document count in the index"""
        try:
            results = self.search_client.search(
                search_text="*",
                include_total_count=True,
                top=1
            )
            return results.get_count()
        except Exception as e:
            print(f"❌ Error getting document count: {str(e)}")
            return -1
    
    def validate_indexer_configuration(self) -> bool:
        """Validate indexer configuration"""
        try:
            indexer = self.indexer_client.get_indexer(self.indexer_name)
            print(f"📋 Indexer Configuration:")
            print(f"   Name: {indexer.name}")
            print(f"   Description: {indexer.description}")
            print(f"   Target Index: {indexer.target_index_name}")
            print(f"   Data Source: {indexer.data_source_name}")
            print(f"   Skillset: {indexer.skillset_name}")
            
            # Check if it matches our updated skillset
            if indexer.skillset_name == self.skillset_name:
                print("✅ Indexer is using the correct updated skillset")
            else:
                print(f"⚠️  Indexer skillset mismatch: {indexer.skillset_name} vs {self.skillset_name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error validating indexer: {str(e)}")
            return False

def main():
    """Main execution function"""
    print("🤖 AZURE SEARCH INDEXER RESET & RUN")
    print("=" * 60)
    print("Resetting indexer and processing clean, standardized documents")
    print()
    
    try:
        # Initialize manager
        manager = IndexerManager()
        
        print("✅ Connected to Azure Search successfully")
        print(f"   Service: {manager.endpoint}")
        print(f"   Indexer: {manager.indexer_name}")
        print(f"   Index: {manager.index_name}")
        print()
        
        # Step 1: Check current status
        print("🔍 Step 1: Checking current indexer status...")
        initial_status = manager.get_indexer_status()
        if "error" in initial_status:
            print(f"❌ Error getting indexer status: {initial_status['error']}")
            return False
        
        print(f"   Current status: {initial_status.get('status', 'unknown')}")
        print(f"   Last result: {initial_status.get('last_result', 'none')}")
        print()
        
        # Step 2: Validate configuration
        print("📋 Step 2: Validating indexer configuration...")
        if not manager.validate_indexer_configuration():
            print("❌ Configuration validation failed")
            return False
        print()
        
        # Step 3: Check initial document count
        print("📊 Step 3: Checking current document count...")
        initial_count = manager.get_document_count()
        print(f"   Current documents in index: {initial_count:,}")
        print()
        
        # Step 4: Reset indexer
        print("🔄 Step 4: Resetting indexer...")
        if not manager.reset_indexer():
            print("❌ Indexer reset failed")
            return False
        print()
        
        # Step 5: Run indexer
        print("▶️  Step 5: Running indexer with clean data...")
        if not manager.run_indexer():
            print("❌ Failed to start indexer")
            return False
        print()
        
        # Step 6: Monitor progress
        print("📊 Step 6: Monitoring indexer progress...")
        result = manager.monitor_indexer_progress(max_wait_minutes=30)
        
        if result.get("completed"):
            print("✅ Indexer execution completed!")
            print(f"   Items processed: {result.get('items_processed', 'unknown')}")
            print(f"   Failed items: {result.get('failed_items', 0)}")
            print(f"   Warnings: {result.get('warnings', 0)}")
            
            if result.get("has_failures"):
                print("⚠️  Some items failed during processing")
        else:
            print(f"⚠️  Monitoring ended: {result.get('message', 'Unknown reason')}")
        
        print()
        
        # Step 7: Final document count
        print("📊 Step 7: Checking final document count...")
        final_count = manager.get_document_count()
        print(f"   Final documents in index: {final_count:,}")
        
        if final_count > initial_count:
            print(f"✅ Document count increased by {final_count - initial_count:,}")
        elif final_count == initial_count:
            print("⚠️  Document count unchanged - may need investigation")
        else:
            print("⚠️  Document count decreased - may indicate issues")
        
        print("\n🎉 INDEXER RESET AND RUN COMPLETED!")
        print("=" * 60)
        print("📋 Next steps:")
        print("   1. Test search functionality with updated index")
        print("   2. Verify client metadata filtering works")
        print("   3. Check for any indexing warnings or errors")
        print("   4. Monitor search performance")
        
        return True
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)