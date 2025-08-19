#!/usr/bin/env python3
"""
AOAI-RAG Flask API Server - FIXED VERSION

Fixed the chat endpoint to properly use search results.
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import dataclass, asdict

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from azure.search.documents.aio import SearchClient
    from azure.search.documents.models import VectorizedQuery
    from azure.core.credentials import AzureKeyCredential
    from openai import AsyncAzureOpenAI
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Missing required packages: {e}")
    print("💡 Run: pip install -r requirements.txt")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SearchResult:
    """Search result structure"""
    id: str
    content: str
    sourcefile: str
    sourcepage: str
    score: float
    title: str = ""

class AOAIRAGService:
    """RAG service that powers the API endpoints"""
    
    def __init__(self):
        self.config = self._load_config()
        self._initialize_clients()
        
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
    
    def _initialize_clients(self):
        """Initialize Azure clients"""
        self.search_client = SearchClient(
            endpoint=f"https://{self.config['search_service']}.search.windows.net",
            index_name=self.config['index_name'],
            credential=AzureKeyCredential(self.config['search_key'])
        )
        
        self.openai_client = AsyncAzureOpenAI(
            api_key=self.config['openai_key'],
            api_version=self.config['openai_version'],
            azure_endpoint=self.config['openai_endpoint']
        )
    
    async def search_documents(self, query: str, top_k: int = 5) -> List[SearchResult]:
        """Search for relevant documents"""
        try:
            logger.info(f"🔍 Searching for: '{query}' (top_k: {top_k})")
            
            # Generate embedding
            embedding_response = await self.openai_client.embeddings.create(
                input=query,
                model=self.config['embedding_model']
            )
            query_vector = embedding_response.data[0].embedding
            logger.info(f"✅ Embedding generated ({len(query_vector)} dimensions)")
            
            # Create vector query
            vector_query = VectorizedQuery(
                vector=query_vector,
                k_nearest_neighbors=top_k,
                fields="text_vector"
            )
            
            # Hybrid search
            search_results = await self.search_client.search(
                search_text=query,
                vector_queries=[vector_query],
                top=top_k
            )
            
            # Process results
            results = []
            async for result in search_results:
                search_result = SearchResult(
                    id=result.get("chunk_id", ""),
                    content=result.get("chunk", ""),
                    sourcefile=result.get("filename", ""),
                    sourcepage=result.get("document_path", ""),
                    score=result.get("@search.score", 0.0),
                    title=result.get("title", "")
                )
                results.append(search_result)
            
            logger.info(f"✅ Found {len(results)} search results")
            return results
            
        except Exception as e:
            logger.error(f"❌ Search failed: {str(e)}")
            return []
    
    async def generate_rag_response(self, question: str, search_results: List[SearchResult]) -> str:
        """Generate RAG response from search results"""
        try:
            if not search_results:
                logger.warning("No search results provided for RAG response")
                return "I couldn't find any relevant information to answer your question."
            
            logger.info(f"💬 Generating RAG response with {len(search_results)} sources")
            
            # Build context
            context_sources = []
            for i, result in enumerate(search_results[:3], 1):
                source = f"Source {i} [{result.sourcefile}]: {result.content[:500]}..."
                context_sources.append(source)
            
            context = "\n\n".join(context_sources)
            
            # Create prompt
            system_prompt = f"""You are Jennifur, an AI assistant that helps with company information.

Answer the user's question using ONLY the information from the sources below.
Always cite your sources using [filename] format.
Be helpful, concise, and professional.
If the sources don't contain enough information, say so clearly.

SOURCES:
{context}"""
            
            # Generate response
            response = await self.openai_client.chat.completions.create(
                model=self.config['chat_model'],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": question}
                ],
                max_tokens=500,
                temperature=0.3
            )
            
            rag_response = response.choices[0].message.content
            logger.info(f"✅ RAG response generated successfully")
            return rag_response
            
        except Exception as e:
            logger.error(f"❌ Response generation failed: {str(e)}")
            return f"I apologize, but I encountered an error while generating a response: {str(e)}"

# Initialize the RAG service
rag_service = AOAIRAGService()

# Create Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "AOAI-RAG API",
        "version": "1.0.1",
        "chat_model": rag_service.config['chat_model'],
        "search_index": rag_service.config['index_name']
    })

@app.route('/api/search', methods=['POST'])
def search():
    """Search endpoint for finding relevant documents"""
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({"error": "Missing 'query' in request body"}), 400
        
        query = data['query'].strip()
        top_k = data.get('top_k', 5)
        
        if not query:
            return jsonify({"error": "Query cannot be empty"}), 400
        
        logger.info(f"🔍 Search request: '{query}' (top_k: {top_k})")
        
        # Run async search
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(rag_service.search_documents(query, top_k))
        finally:
            loop.close()
        
        # Convert to JSON-serializable format
        results_data = [asdict(result) for result in results]
        
        response = {
            "query": query,
            "results": results_data,
            "total_results": len(results_data)
        }
        
        logger.info(f"✅ Search completed: {len(results)} results found")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Search endpoint error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat', methods=['POST'])
def chat():
    """Chat endpoint for RAG question-answering - FIXED VERSION"""
    try:
        data = request.get_json()
        if not data or 'messages' not in data:
            return jsonify({"error": "Missing 'messages' in request body"}), 400
        
        messages = data['messages']
        if not messages or not isinstance(messages, list):
            return jsonify({"error": "Messages must be a non-empty list"}), 400
        
        # Get the last user message
        user_message = messages[-1]['content'] if messages else ""
        if not user_message.strip():
            return jsonify({"error": "User message cannot be empty"}), 400
        
        # Get search parameters
        overrides = data.get('overrides', {})
        top_k = overrides.get('top_k', 5)
        
        logger.info(f"💬 Chat request: '{user_message[:50]}...' (top_k: {top_k})")
        
        # Run async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Search for relevant documents - FIXED: This was working in search endpoint
            search_results = loop.run_until_complete(
                rag_service.search_documents(user_message, top_k)
            )
            
            logger.info(f"🔍 Chat search found {len(search_results)} results")
            
            # Generate RAG response
            rag_response = loop.run_until_complete(
                rag_service.generate_rag_response(user_message, search_results)
            )
        finally:
            loop.close()
        
        # Build response - FIXED: Include the search results properly
        response = {
            "message": {
                "content": rag_response,
                "role": "assistant"
            },
            "context": {
                "data_points": [result.content for result in search_results],
                "sources": [
                    {
                        "sourcefile": result.sourcefile,
                        "sourcepage": result.sourcepage,
                        "score": result.score,
                        "content_preview": result.content[:200] + "..." if len(result.content) > 200 else result.content
                    } for result in search_results
                ],
                "search_query": user_message,
                "total_sources": len(search_results)
            }
        }
        
        logger.info(f"✅ Chat completed: Used {len(search_results)} sources")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Chat endpoint error: {str(e)}")
        return jsonify({
            "error": str(e),
            "message": {
                "content": "I apologize, but I encountered an error while processing your request.",
                "role": "assistant"
            }
        }), 500

@app.route('/api/stats', methods=['GET'])
def stats():
    """Get system statistics"""
    try:
        # Basic stats
        stats_data = {
            "system": {
                "search_service": rag_service.config['search_service'],
                "search_index": rag_service.config['index_name'],
                "chat_model": rag_service.config['chat_model'],
                "embedding_model": rag_service.config['embedding_model']
            },
            "capabilities": {
                "hybrid_search": True,
                "vector_search": True,
                "rag_chat": True,
                "source_citations": True
            },
            "status": "operational"
        }
        
        return jsonify(stats_data)
        
    except Exception as e:
        logger.error(f"❌ Stats endpoint error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({"error": "Method not allowed"}), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

def main():
    """Main function to run the Flask app"""
    print("🚀 Starting AOAI-RAG API Server - FIXED VERSION...")
    print(f"📚 Search Index: {rag_service.config['index_name']}")
    print(f"🤖 Chat Model: {rag_service.config['chat_model']}")
    print(f"🧠 Embedding Model: {rag_service.config['embedding_model']}")
    print("\n📡 Available endpoints:")
    print("   GET  /api/health     - Health check")
    print("   POST /api/search     - Search documents")
    print("   POST /api/chat       - RAG chat (FIXED)")
    print("   GET  /api/stats      - System stats")
    print("\n💡 Test with:")
    print("   curl http://localhost:5001/api/health")
    print("\n🌐 Ready for your HTML frontend to connect!")
    
    # Run the Flask app on port 5001 (since that's where it's running)
    app.run(
        debug=True,
        host='0.0.0.0',
        port=5001,  # Changed to match your running server
        threaded=True
    )

if __name__ == "__main__":
    main()