#!/usr/bin/env python3
"""
Reset and Run Indexer without Skillset
Run the indexer with no skillset to eliminate chunk field warnings
"""

import os
import time
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class NoSkillsetIndexerManager:
    """Manage indexer without skillset"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.indexer_name = "jennifur-rag-indexer"
        self.index_name = "jennifur-rag"
        
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
    
    def get_document_count(self) -> int:
        """Get current document count"""
        try:
            results = self.search_client.search("*", include_total_count=True, top=1)
            return results.get_count()
        except Exception as e:
            print(f"❌ Error getting document count: {str(e)}")
            return -1
    
    def reset_indexer(self) -> bool:
        """Reset the indexer"""
        try:
            print(f"🔄 Resetting indexer '{self.indexer_name}'...")
            self.indexer_client.reset_indexer(self.indexer_name)
            print("✅ Indexer reset successfully")
            return True
        except Exception as e:
            print(f"❌ Error resetting indexer: {str(e)}")
            return False
    
    def run_indexer(self) -> bool:
        """Run the indexer"""
        try:
            print(f"▶️  Running indexer '{self.indexer_name}' (no skillset)...")
            self.indexer_client.run_indexer(self.indexer_name)
            print("✅ Indexer started successfully")
            return True
        except Exception as e:
            print(f"❌ Error running indexer: {str(e)}")
            return False
    
    def check_quick_status(self) -> dict:
        """Quick status check"""
        try:
            status = self.indexer_client.get_indexer_status(self.indexer_name)
            return {
                "status": str(status.status),
                "last_result": str(status.last_result.status) if status.last_result else "none",
                "items_processed": getattr(status.last_result, 'item_count', 0) if status.last_result else 0,
                "errors": getattr(status.last_result, 'failed_item_count', 0) if status.last_result else 0,
                "warnings": getattr(status.last_result, 'warning_count', 0) if status.last_result else 0
            }
        except Exception as e:
            return {"error": str(e)}

def main():
    """Main execution function"""
    print("🤖 NO-SKILLSET INDEXER RESET & RUN")
    print("=" * 50)
    print("Running indexer without skillset to eliminate chunk field warnings")
    print()
    
    try:
        manager = NoSkillsetIndexerManager()
        
        print(f"✅ Connected to Azure Search: {manager.endpoint}")
        print(f"   Indexer: {manager.indexer_name}")
        print(f"   Index: {manager.index_name}")
        print()
        
        # Check initial state
        print("📊 Step 1: Initial document count")
        initial_count = manager.get_document_count()
        print(f"   Current documents: {initial_count:,}")
        print()
        
        # Reset indexer
        print("🔄 Step 2: Reset indexer")
        if not manager.reset_indexer():
            return False
        
        # Wait a moment
        print("⏳ Waiting 10 seconds for reset...")
        time.sleep(10)
        print()
        
        # Run indexer
        print("▶️  Step 3: Run indexer without skillset")
        if not manager.run_indexer():
            return False
        print()
        
        # Monitor briefly
        print("📊 Step 4: Quick monitoring")
        for i in range(6):  # Check 6 times over 3 minutes
            time.sleep(30)
            status = manager.check_quick_status()
            
            if "error" in status:
                print(f"   ❌ Status check error: {status['error']}")
                continue
            
            print(f"   Check {i+1}/6: Status={status['status']}, Result={status['last_result']}")
            print(f"     Items: {status['items_processed']}, Errors: {status['errors']}, Warnings: {status['warnings']}")
            
            if status['last_result'].lower() in ['success', 'transientfailure']:
                print(f"\n✅ Indexer completed: {status['last_result']}")
                if status['warnings'] == 0:
                    print("🎉 NO WARNINGS! Chunk field issue resolved!")
                else:
                    print(f"⚠️  Still {status['warnings']} warnings - may need further investigation")
                break
        
        # Final count
        print(f"\n📊 Step 5: Final document count")
        final_count = manager.get_document_count()
        print(f"   Final documents: {final_count:,}")
        print(f"   Change: {final_count - initial_count:+,}")
        
        print("\n🎉 NO-SKILLSET INDEXER RUN COMPLETE!")
        print("=" * 50)
        print("The indexer ran without skillset processing.")
        print("Check Azure Portal for detailed warnings/errors if any remain.")
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()