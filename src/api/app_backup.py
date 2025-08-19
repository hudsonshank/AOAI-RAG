"""
Enhanced Flask API with proper environment loading
"""

import os
import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load environment first
load_dotenv()

from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from openai import AsyncAzureOpenAI
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from core.query_optimizer import AdvancedQueryOptimizer

app = Flask(__name__)
conversation_memory = {}

class EnhancedRAGEngine:
    def __init__(self):
        # Initialize Azure clients
        self.search_client = SearchClient(
            endpoint=os.getenv("AZURE_SEARCH_ENDPOINT"),
            index_name=os.getenv("EXISTING_INDEX_NAME"),
            credential=AzureKeyCredential(os.getenv("AZURE_SEARCH_ADMIN_KEY"))
        )
        
        self.openai_client = AsyncAzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        self.chat_model = os.getenv("AZURE_OPENAI_CHAT_MODEL")
        self.query_optimizer = AdvancedQueryOptimizer()
        print("✅ Enhanced RAG Engine initialized")
    
    async def enhanced_chat(self, messages: List[Dict], session_id: str = "default", overrides: Dict = None) -> Dict:
        """Enhanced chat with optimization and memory"""
        thoughts = []
        start_time = datetime.utcnow()
        
        try:
            user_query = messages[-1]["content"]
            conversation_history = conversation_memory.get(session_id, [])
            
            # Step 1: Query Optimization
            thoughts.append({"title": "Query Optimization", "description": "Optimizing search query", "timestamp": datetime.utcnow().isoformat()})
            
            optimization_result = await self.query_optimizer.optimize_query(user_query, conversation_history)
            optimized_query = optimization_result["optimized_query"]
            
            thoughts.append({"title": "Query Enhanced", "description": f"'{user_query}' → '{optimized_query}'", "timestamp": datetime.utcnow().isoformat()})
            
            # Step 2: Search
            thoughts.append({"title": "Document Search", "description": "Searching knowledge base", "timestamp": datetime.utcnow().isoformat()})
            
            search_results = await self.search_documents(optimized_query, overrides or {})
            
            thoughts.append({"title": "Sources Found", "description": f"Retrieved {len(search_results['sources'])} sources", "timestamp": datetime.utcnow().isoformat()})
            
            # Step 3: Generate Response
            thoughts.append({"title": "Answer Generation", "description": "Creating response with citations", "timestamp": datetime.utcnow().isoformat()})
            
            response = await self.generate_response(user_query, search_results["sources"])
            
            # Simple follow-up questions
            followup_questions = [
                f"Can you provide more details about {user_query.lower()}?",
                "Are there related policies I should know about?",
                "What are the next steps?"
            ]
            
            # Update memory
            conversation_memory[session_id] = conversation_history + [
                {"role": "user", "content": user_query},
                {"role": "assistant", "content": response["content"]}
            ][-20:]  # Keep last 20 messages
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            return {
                "message": response,
                "context": {
                    "thoughts": thoughts,
                    "data_points": search_results["sources"],
                    "followup_questions": followup_questions,
                    "optimization": optimization_result,
                    "session_id": session_id,
                    "processing_time": f"{processing_time:.2f}s"
                }
            }
            
        except Exception as e:
            return {
                "message": {"content": f"Error: {str(e)}", "role": "assistant"},
                "context": {"thoughts": thoughts, "error": str(e)}
            }
    
    async def search_documents(self, query: str, overrides: Dict) -> Dict:
        """Search documents"""
        try:
            results = self.search_client.search(
                search_text=query,
                top=overrides.get("top", 5)
            )
            
            sources = []
            for result in results:
                sources.append({
                    "content": result.get("chunk", ""),
                    "content_preview": result.get("chunk", "")[:200] + "...",
                    "sourcefile": result.get("filename", ""),
                    "sourcepage": result.get("document_path", ""),
                    "score": float(result.get("@search.score", 0))
                })
            
            return {"sources": sources, "search_query": query}
            
        except Exception as e:
            return {"sources": [], "search_query": query, "error": str(e)}
    
    async def generate_response(self, user_query: str, sources: List[Dict]) -> Dict:
        """Generate response with citations"""
        try:
            context = "\n\n".join([
                f"Source: {s.get('sourcefile', 'Unknown')}\n{s.get('content', '')}"
                for s in sources[:3]
            ])
            
            messages = [
                {"role": "system", "content": f"Answer based on these sources. Include citations [filename].\n\nSources:\n{context}"},
                {"role": "user", "content": user_query}
            ]
            
            response = await self.openai_client.chat.completions.create(
                model=self.chat_model,
                messages=messages,
                temperature=0.3,
                max_tokens=500
            )
            
            return {"content": response.choices[0].message.content, "role": "assistant"}
            
        except Exception as e:
            return {"content": f"Found relevant information but couldn't generate response: {str(e)}", "role": "assistant"}

# Initialize engine
rag_engine = EnhancedRAGEngine()

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "status": "healthy",
        "version": "enhanced-v1.0",
        "features": ["query_optimization", "conversation_memory", "thought_transparency"]
    })

@app.route('/api/chat', methods=['POST'])
def chat():
    try:
        data = request.get_json()
        messages = data.get("messages", [])
        session_id = data.get("session_id", "default")
        overrides = data.get("overrides", {})
        
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(rag_engine.enhanced_chat(messages, session_id, overrides))
        finally:
            loop.close()
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("🚀 Enhanced RAG API Server Starting...")
    print("✅ Environment loaded")
    print("🌐 Health: http://localhost:5000/api/health")
    app.run(debug=True, host='0.0.0.0', port=5000)
