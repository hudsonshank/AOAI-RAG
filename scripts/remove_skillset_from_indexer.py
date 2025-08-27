#!/usr/bin/env python3
"""
Remove Skillset from Indexer
Since documents are already properly processed, remove skillset to eliminate warnings
"""

import os
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexerClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class IndexerSkillsetRemover:
    """Remove skillset from indexer configuration"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.indexer_name = "jennifur-rag-indexer"
        
        if not all([self.endpoint, self.admin_key]):
            raise ValueError("Missing required Azure Search configuration")
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.indexer_client = SearchIndexerClient(
            endpoint=self.endpoint,
            credential=self.credential
        )
    
    def remove_skillset_from_indexer(self) -> bool:
        """Remove skillset from indexer configuration"""
        try:
            print(f"🔧 Removing skillset from indexer '{self.indexer_name}'...")
            
            # Get the existing indexer
            indexer = self.indexer_client.get_indexer(self.indexer_name)
            
            print(f"   Current skillset: {indexer.skillset_name}")
            print(f"   Target index: {indexer.target_index_name}")
            print(f"   Data source: {indexer.data_source_name}")
            
            # Remove the skillset
            old_skillset = indexer.skillset_name
            indexer.skillset_name = None
            
            # Update the indexer
            result = self.indexer_client.create_or_update_indexer(indexer)
            
            print(f"✅ Skillset removed successfully!")
            print(f"   Removed skillset: {old_skillset}")
            print(f"   Updated indexer: {result.name}")
            print(f"   Now using: No skillset (direct indexing)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error removing skillset: {str(e)}")
            return False
    
    def show_indexer_config(self) -> None:
        """Show current indexer configuration"""
        try:
            indexer = self.indexer_client.get_indexer(self.indexer_name)
            print(f"📋 Current indexer configuration:")
            print(f"   Name: {indexer.name}")
            print(f"   Target Index: {indexer.target_index_name}")
            print(f"   Data Source: {indexer.data_source_name}")
            print(f"   Skillset: {indexer.skillset_name or 'None'}")
            print(f"   Description: {indexer.description or 'N/A'}")
        except Exception as e:
            print(f"❌ Error getting indexer config: {str(e)}")

def main():
    """Main execution function"""
    print("🤖 INDEXER SKILLSET REMOVER")
    print("=" * 50)
    print("Removing skillset from indexer since documents are pre-processed")
    print()
    
    try:
        remover = IndexerSkillsetRemover()
        
        print(f"✅ Connected to Azure Search: {remover.endpoint}")
        print(f"   Indexer: {remover.indexer_name}")
        print()
        
        # Show current config
        print("🔍 Step 1: Current indexer configuration")
        remover.show_indexer_config()
        print()
        
        # Remove skillset
        print("🔧 Step 2: Removing skillset from indexer")
        if remover.remove_skillset_from_indexer():
            print()
            
            # Show updated config
            print("✅ Step 3: Updated indexer configuration")
            remover.show_indexer_config()
            
            print("\n🎉 SKILLSET REMOVED SUCCESSFULLY!")
            print("=" * 50)
            print("✅ Indexer now processes documents directly without skillset")
            print("✅ This should eliminate the chunk field warnings")
            print("✅ Documents already have proper metadata and structure")
            print()
            print("📋 Next steps:")
            print("   1. Reset the indexer to clear cached state")  
            print("   2. Run the indexer to process documents without skillset")
            print("   3. Verify no warnings occur")
        else:
            print("\n❌ SKILLSET REMOVAL FAILED!")
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()