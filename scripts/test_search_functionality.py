#!/usr/bin/env python3
"""
Test Search Functionality
Verify that search works correctly after indexer updates
"""

import os
from dotenv import load_dotenv
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class SearchTester:
    """Test search functionality"""
    
    def __init__(self):
        self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        self.admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        self.index_name = "jennifur-rag"
        
        if not all([self.endpoint, self.admin_key]):
            raise ValueError("Missing required Azure Search configuration")
        
        self.credential = AzureKeyCredential(self.admin_key)
        self.search_client = SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=self.credential
        )
    
    def test_basic_search(self) -> bool:
        """Test basic search functionality"""
        try:
            print("🔍 Testing basic search...")
            
            results = self.search_client.search(
                search_text="policy",
                top=5,
                include_total_count=True
            )
            
            result_list = list(results)
            total_count = results.get_count()
            
            print(f"   Query: 'policy'")
            print(f"   Total matches: {total_count:,}")
            print(f"   Retrieved: {len(result_list)}")
            
            if result_list:
                print(f"   Top result score: {result_list[0].get('@search.score', 'N/A')}")
                print(f"   Top result file: {result_list[0].get('filename', 'N/A')}")
                print(f"   Has chunk content: {'chunk' in result_list[0]}")
                print(f"   Has client metadata: {'client_name' in result_list[0]}")
            
            return len(result_list) > 0
            
        except Exception as e:
            print(f"❌ Basic search error: {str(e)}")
            return False
    
    def test_client_filtering(self) -> bool:
        """Test client-specific filtering"""
        try:
            print("\n🏢 Testing client filtering...")
            
            # Test filtering by client
            results = self.search_client.search(
                search_text="*",
                filter="client_name eq 'Autobahn Internal'",
                top=5,
                include_total_count=True
            )
            
            result_list = list(results)
            total_count = results.get_count()
            
            print(f"   Filter: client_name eq 'Autobahn Internal'")
            print(f"   Total matches: {total_count:,}")
            print(f"   Retrieved: {len(result_list)}")
            
            if result_list:
                print(f"   Client of first result: {result_list[0].get('client_name', 'N/A')}")
            
            return len(result_list) > 0
            
        except Exception as e:
            print(f"❌ Client filtering error: {str(e)}")
            return False
    
    def test_field_availability(self) -> dict:
        """Test what fields are available in search results"""
        try:
            print("\n📋 Testing field availability...")
            
            results = self.search_client.search(
                search_text="*",
                top=1
            )
            
            result_list = list(results)
            if result_list:
                doc = result_list[0]
                fields = list(doc.keys())
                
                # Check for key fields
                key_fields = [
                    'chunk', 'chunk_id', 'filename', 'document_path',
                    'client_name', 'pm_initial', 'document_category',
                    'is_client_specific', 'metadata_updated_timestamp'
                ]
                
                field_status = {}
                for field in key_fields:
                    field_status[field] = field in fields
                
                print(f"   Total fields available: {len(fields)}")
                print(f"   Key field availability:")
                for field, available in field_status.items():
                    status = "✅" if available else "❌"
                    print(f"     {status} {field}")
                
                return field_status
            else:
                print("   No documents returned")
                return {}
                
        except Exception as e:
            print(f"❌ Field availability error: {str(e)}")
            return {}

def main():
    """Main test execution"""
    print("🧪 SEARCH FUNCTIONALITY TEST")
    print("=" * 50)
    print("Testing search after indexer updates")
    print()
    
    try:
        tester = SearchTester()
        
        print(f"✅ Connected to search index: {tester.index_name}")
        print(f"   Endpoint: {tester.endpoint}")
        print()
        
        # Run tests
        basic_works = tester.test_basic_search()
        client_works = tester.test_client_filtering()
        fields = tester.test_field_availability()
        
        print(f"\n📊 TEST RESULTS")
        print("=" * 30)
        print(f"✅ Basic search: {'PASS' if basic_works else 'FAIL'}")
        print(f"✅ Client filtering: {'PASS' if client_works else 'FAIL'}")
        print(f"✅ Key fields available: {sum(fields.values())}/{len(fields)}")
        
        if basic_works and client_works and sum(fields.values()) >= 8:
            print(f"\n🎉 ALL TESTS PASSED!")
            print("Search functionality is working correctly after updates.")
        else:
            print(f"\n⚠️  SOME TESTS FAILED!")
            print("Further investigation may be needed.")
        
    except Exception as e:
        print(f"❌ Test execution error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()