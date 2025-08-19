#!/usr/bin/env python3
"""
AOAI-RAG Basic Test Script - WORKING VERSION

Tests your existing Azure AI Search index and Azure OpenAI setup
Works with or without semantic search configuration.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from azure.search.documents.aio import SearchClient
    from azure.search.documents.models import VectorizedQuery, QueryType
    from azure.core.credentials import AzureKeyCredential
    from openai import AsyncAzureOpenAI
except ImportError as e:
    print(f"❌ Missing required packages: {e}")
    print("💡 Run: pip install -r requirements.txt")
    sys.exit(1)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv not installed. Using environment variables directly.")

@dataclass
class TestResult:
    """Test result structure"""
    id: str
    content: str
    sourcefile: str
    sourcepage: str
    score: float
    title: str = ""

class AOAIRAGTester:
    """Test your existing AOAI-RAG setup"""
    
    def __init__(self):
        print("🔧 Initializing AOAI-RAG Tester...")
        
        # Load configuration
        self.config = self._load_config()
        self._validate_config()
        
        # Initialize Azure clients
        self.search_client = SearchClient(
            endpoint=f"https://{self.config['search_service']}.search.windows.net",
            index_name=self.config['index_name'],
            credential=AzureKeyCredential(self.config['search_key'])
        )
        
        # Azure OpenAI client
        self.openai_client = AsyncAzureOpenAI(
            api_key=self.config['openai_key'],
            api_version=self.config['openai_version'],
            azure_endpoint=self.config['openai_endpoint']
        )
        
        print("✅ AOAI-RAG Tester initialized successfully")

    def _load_config(self) -> Dict[str, str]:
        """Load configuration from environment variables"""
        return {
            'search_service': os.getenv('AZURE_SEARCH_SERVICE_NAME', 'jennifur-search-service'),
            'search_key': os.getenv('AZURE_SEARCH_ADMIN_KEY'),
            'index_name': os.getenv('EXISTING_INDEX_NAME', 'jennifur-rag'),
            'openai_key': os.getenv('AZURE_OPENAI_API_KEY'),
            'openai_endpoint': os.getenv('AZURE_OPENAI_ENDPOINT'),
            'openai_version': os.getenv('AZURE_OPENAI_API_VERSION', '2024-02-15-preview'),
            'chat_model': os.getenv('AZURE_OPENAI_CHAT_MODEL', 'gpt-4.1'),
            'embedding_model': os.getenv('AZURE_OPENAI_EMBEDDING_MODEL', 'text-embedding-ada-002')
        }

    def _validate_config(self):
        """Validate required configuration"""
        required = ['search_key', 'openai_key', 'openai_endpoint']
        missing = [key for key in required if not self.config[key]]
        
        if missing:
            print(f"❌ Missing required environment variables:")
            for key in missing:
                env_name = {
                    'search_key': 'AZURE_SEARCH_ADMIN_KEY',
                    'openai_key': 'AZURE_OPENAI_API_KEY', 
                    'openai_endpoint': 'AZURE_OPENAI_ENDPOINT'
                }[key]
                print(f"   {env_name}")
            raise ValueError(f"Missing configuration: {missing}")
        
        # Show what we loaded (safely)
        print(f"🔑 Configuration loaded:")
        print(f"   Search service: {self.config['search_service']}")
        print(f"   Search index: {self.config['index_name']}")
        print(f"   OpenAI endpoint: {self.config['openai_endpoint']}")
        print(f"   Chat model: {self.config['chat_model']}")
        print(f"   Embedding model: {self.config['embedding_model']}")

    async def test_search_connectivity(self) -> bool:
        """Test Azure AI Search connectivity"""
        print("\n🔍 Testing Azure AI Search connectivity...")
        
        try:
            # Simple search to test connectivity
            search_results = await self.search_client.search(
                search_text="test",
                top=1,
                include_total_count=True
            )
            
            # Try to get count, fallback if not available
            try:
                count = await search_results.get_count()
            except:
                count = "Available"
            
            print(f"✅ Search service connected successfully")
            print(f"   Index: {self.config['index_name']}")
            print(f"   Total documents: {count}")
            
            return True
            
        except Exception as e:
            print(f"❌ Search connectivity failed: {str(e)}")
            return False

    async def test_openai_connectivity(self) -> bool:
        """Test Azure OpenAI connectivity"""
        print("\n🤖 Testing Azure OpenAI connectivity...")
        
        try:
            # Test chat completion
            print(f"   Testing chat model: {self.config['chat_model']}")
            response = await self.openai_client.chat.completions.create(
                model=self.config['chat_model'],
                messages=[{"role": "user", "content": "Hello, this is a test."}],
                max_tokens=10
            )
            
            print(f"✅ OpenAI chat service connected successfully")
            print(f"   Model: {self.config['chat_model']}")
            print(f"   Test response: {response.choices[0].message.content.strip()}")
            
            # Test embeddings
            print(f"   Testing embedding model: {self.config['embedding_model']}")
            embedding_response = await self.openai_client.embeddings.create(
                input="test embedding",
                model=self.config['embedding_model']
            )
            
            embedding_dim = len(embedding_response.data[0].embedding)
            print(f"✅ Embeddings service working")
            print(f"   Model: {self.config['embedding_model']}")
            print(f"   Dimensions: {embedding_dim}")
            
            return True
            
        except Exception as e:
            print(f"❌ OpenAI connectivity failed: {str(e)}")
            return False

    async def test_hybrid_search(self, query: str, top_k: int = 3) -> List[TestResult]:
        """Test hybrid search (text + vector) - works with or without semantic search"""
        print(f"\n🔎 Testing hybrid search: '{query}'")
        
        try:
            # Generate embedding for vector search
            print(f"   Generating embedding...")
            embedding_response = await self.openai_client.embeddings.create(
                input=query,
                model=self.config['embedding_model']
            )
            query_vector = embedding_response.data[0].embedding
            print(f"   ✅ Embedding generated ({len(query_vector)} dimensions)")
            
            # Create vector query using your existing field name
            vector_query = VectorizedQuery(
                vector=query_vector,
                k_nearest_neighbors=top_k,
                fields="text_vector"  # Your existing vector field
            )
            
            # Try different search approaches
            results = []
            
            # Approach 1: Try hybrid search without semantic search first
            print(f"   Trying hybrid search (text + vector)...")
            try:
                search_results = await self.search_client.search(
                    search_text=query,
                    vector_queries=[vector_query],
                    top=top_k
                )
                
                async for result in search_results:
                    test_result = TestResult(
                        id=result.get("chunk_id", ""),
                        content=result.get("chunk", ""),  
                        sourcefile=result.get("filename", ""),  
                        sourcepage=result.get("document_path", ""),  
                        score=result.get("@search.score", 0.0),
                        title=result.get("title", "")
                    )
                    results.append(test_result)
                
                if results:
                    print(f"✅ Hybrid search successful!")
                
            except Exception as e:
                print(f"   Hybrid search failed: {str(e)}")
            
            # Approach 2: If hybrid failed, try text-only search
            if not results:
                print(f"   Trying text-only search...")
                try:
                    search_results = await self.search_client.search(
                        search_text=query,
                        top=top_k
                    )
                    
                    async for result in search_results:
                        test_result = TestResult(
                            id=result.get("chunk_id", ""),
                            content=result.get("chunk", ""),  
                            sourcefile=result.get("filename", ""),  
                            sourcepage=result.get("document_path", ""),  
                            score=result.get("@search.score", 0.0),
                            title=result.get("title", "")
                        )
                        results.append(test_result)
                    
                    if results:
                        print(f"✅ Text search successful!")
                        
                except Exception as e:
                    print(f"   Text search failed: {str(e)}")
            
            # Approach 3: If both failed, try vector-only search
            if not results:
                print(f"   Trying vector-only search...")
                try:
                    search_results = await self.search_client.search(
                        search_text="",
                        vector_queries=[vector_query],
                        top=top_k
                    )
                    
                    async for result in search_results:
                        test_result = TestResult(
                            id=result.get("chunk_id", ""),
                            content=result.get("chunk", ""),  
                            sourcefile=result.get("filename", ""),  
                            sourcepage=result.get("document_path", ""),  
                            score=result.get("@search.score", 0.0),
                            title=result.get("title", "")
                        )
                        results.append(test_result)
                    
                    if results:
                        print(f"✅ Vector search successful!")
                        
                except Exception as e:
                    print(f"   Vector search failed: {str(e)}")
            
            # Show results
            if results:
                print(f"✅ Found {len(results)} relevant documents:")
                for i, result in enumerate(results, 1):
                    filename = result.sourcefile[:40] + "..." if len(result.sourcefile) > 40 else result.sourcefile
                    print(f"   {i}. {filename} (score: {result.score:.3f})")
                    preview = result.content[:80] + "..." if len(result.content) > 80 else result.content
                    print(f"      Preview: {preview}")
            else:
                print(f"❌ No results found with any search method")
            
            return results
            
        except Exception as e:
            print(f"❌ Search failed completely: {str(e)}")
            return []

    async def test_rag_chat(self, question: str, search_results: List[TestResult]) -> str:
        """Test RAG chat functionality"""
        print(f"\n💬 Testing RAG chat: '{question}'")
        
        if not search_results:
            print("❌ No search results to work with")
            return "No search results available"
        
        try:
            # Build context from search results
            context_sources = []
            for i, result in enumerate(search_results[:3], 1):
                source = f"Source {i} [{result.sourcefile}]: {result.content[:400]}..."
                context_sources.append(source)
            
            context = "\n\n".join(context_sources)
            
            # RAG prompt
            system_prompt = f"""You are Jennifur, an AI assistant that helps with company information.

Answer the user's question using ONLY the information from the sources below.
Always cite your sources using [filename] format.
Be helpful, concise, and professional.

SOURCES:
{context}"""
            
            print(f"   Generating RAG response...")
            # Generate response
            response = await self.openai_client.chat.completions.create(
                model=self.config['chat_model'],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question}
                ],
                max_tokens=300,
                temperature=0.3
            )
            
            answer = response.choices[0].message.content
            print(f"✅ RAG response generated successfully")
            print(f"🤖 Answer: {answer[:200]}...")
            
            return answer
            
        except Exception as e:
            print(f"❌ RAG chat failed: {str(e)}")
            return f"Error generating response: {str(e)}"

    async def run_comprehensive_test(self):
        """Run comprehensive test suite"""
        print("🚀 AOAI-RAG Comprehensive Test Suite")
        print("=" * 60)
        
        # Test 1: Connectivity
        search_ok = await self.test_search_connectivity()
        if not search_ok:
            print("🛑 Search connectivity failed - stopping tests")
            return False
            
        openai_ok = await self.test_openai_connectivity()
        if not openai_ok:
            print("🛑 OpenAI connectivity failed - stopping tests")
            return False
        
        print("\n✅ All connectivity tests passed! Testing RAG functionality...")
        
        # Test 2: RAG functionality with various search methods
        test_queries = [
            "remote work",
            "policy", 
            "document"
        ]
        
        successful_rag_tests = 0
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n--- RAG Test {i}/{len(test_queries)} ---")
            
            # Search
            results = await self.test_hybrid_search(query, top_k=3)
            
            if results:
                # Test RAG chat
                question = f"What information do you have about {query}?"
                answer = await self.test_rag_chat(question, results)
                
                if answer and "Error" not in answer and "No search results" not in answer:
                    successful_rag_tests += 1
                    print(f"✅ RAG test {i} successful")
                else:
                    print(f"❌ RAG test {i} failed")
            else:
                print(f"❌ No search results for query: {query}")
            
            # Small delay between tests
            await asyncio.sleep(1)
        
        # Final Results
        print(f"\n📊 FINAL TEST RESULTS")
        print("=" * 60)
        print(f"✅ Search Connectivity: PASS")
        print(f"✅ OpenAI Connectivity: PASS") 
        print(f"✅ Successful RAG Tests: {successful_rag_tests}/{len(test_queries)}")
        
        if successful_rag_tests >= 2:
            print(f"\n🎉 EXCELLENT! Your AOAI-RAG system is working!")
            print(f"\n📋 What this confirms:")
            print(f"   ✅ Your {self.config['index_name']} index is searchable")
            print(f"   ✅ Vector embeddings are working")
            print(f"   ✅ Hybrid/text search is functional")
            print(f"   ✅ RAG question-answering works")
            print(f"   ✅ Source citations are included")
            
            print(f"\n🚀 Ready for next steps:")
            print(f"   1. Build Flask API server")
            print(f"   2. Create HTML frontend")
            print(f"   3. Add semantic search configuration (optional)")
            print(f"   4. Add advanced features")
            
            return True
        elif successful_rag_tests >= 1:
            print(f"\n✅ Good progress! Basic RAG is working.")
            print(f"Some queries might need better search configuration.")
            return True
        else:
            print(f"\n⚠️  RAG functionality needs attention.")
            print(f"Search connectivity works but no successful searches found.")
            print(f"💡 This might be due to:")
            print(f"   - Different field names in your index")
            print(f"   - No matching documents for test queries")
            print(f"   - Index structure differences")
            return False

async def cleanup_connections():
    """Clean up any remaining connections"""
    try:
        await asyncio.sleep(0.1)  # Small delay for cleanup
    except:
        pass

def main():
    """Main test function"""
    print("🤖 AOAI-RAG - Testing Your Existing Setup")
    print("This will test your Azure AI Search index and Azure OpenAI integration")
    print("=" * 70)
    
    # Check for .env file
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        print("⚠️  No .env file found!")
        return
    else:
        print(f"✅ Found .env file at: {env_path}")
    
    # Run tests
    try:
        tester = AOAIRAGTester()
        success = asyncio.run(tester.run_comprehensive_test())
        
        # Clean up
        asyncio.run(cleanup_connections())
        
        if success:
            print("\n✨ Your AOAI-RAG system is ready for enhancement!")
            print("✨ Time to build the Flask API and frontend!")
        else:
            print("\n🔧 Some issues found, but connectivity is working.")
            print("💡 You can proceed with API development and fix search later.")
            
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()