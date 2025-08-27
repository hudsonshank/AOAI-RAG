#!/usr/bin/env python3
"""
Update Indexer Configuration
Updates the existing indexer to use the new simplified skillset
"""

import os
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class IndexerConfigUpdater:
    """Update indexer configuration for new skillset"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.new_skillset_name = "jennifur-skillset"  # Our new simplified skillset
        self.indexer_name = "jennifur-rag-indexer"    # Existing indexer
        
        if not all([self.endpoint, self.admin_key]):
            raise ValueError("Missing required Azure Search configuration")
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.indexer_client = SearchIndexerClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
    
    def update_indexer_skillset(self) -> bool:
        """Update the indexer to use the new simplified skillset"""
        try:
            print(f"🔧 Updating indexer '{self.indexer_name}' configuration...")
            
            # Get the existing indexer
            indexer = self.indexer_client.get_indexer(self.indexer_name)
            
            print(f"   Current skillset: {indexer.skillset_name}")
            print(f"   Target index: {indexer.target_index_name}")
            print(f"   Data source: {indexer.data_source_name}")
            
            # Update the skillset name
            old_skillset = indexer.skillset_name
            indexer.skillset_name = self.new_skillset_name
            
            # Update the indexer
            result = self.indexer_client.create_or_update_indexer(indexer)
            
            print(f"✅ Indexer updated successfully!")
            print(f"   Changed skillset: {old_skillset} → {self.new_skillset_name}")
            print(f"   Updated indexer: {result.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error updating indexer: {str(e)}")
            return False

def main():
    """Main execution function"""
    print("🤖 INDEXER CONFIGURATION UPDATER")
    print("=" * 50)
    print("Updating indexer to use the new simplified skillset")
    print()
    
    try:
        updater = IndexerConfigUpdater()
        
        print(f"✅ Connected to Azure Search: {updater.endpoint}")
        print(f"   Indexer: {updater.indexer_name}")
        print(f"   New skillset: {updater.new_skillset_name}")
        print()
        
        if updater.update_indexer_skillset():
            print("\n🎉 INDEXER CONFIGURATION UPDATED!")
            print("The indexer now uses the simplified skillset for new document format")
            print("\nReady to reset and run the indexer with clean data.")
        else:
            print("\n❌ CONFIGURATION UPDATE FAILED!")
            
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()