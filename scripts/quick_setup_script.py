#!/usr/bin/env python3
"""
Quick setup script to create the fixed files
"""

import os
import shutil
from pathlib import Path

def setup_enhanced_rag():
    """Set up the enhanced RAG files"""
    print("🔧 Setting up Enhanced RAG System...")
    
    # Create directories
    directories = ["src/core", "scripts", "src/models"]
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"   ✅ Directory: {directory}")
    
    # Create __init__.py files
    init_files = ["src/__init__.py", "src/core/__init__.py", "src/models/__init__.py"]
    for init_file in init_files:
        Path(init_file).touch()
        print(f"   ✅ Init file: {init_file}")
    
    # Backup existing app.py
    if os.path.exists("src/api/app.py"):
        shutil.copy("src/api/app.py", "src/api/app_backup.py")
        print("   ✅ Backed up existing app.py")
    
    print("\n📝 Files you need to create:")
    print("   1. src/core/query_optimizer.py - Copy from 'Advanced Query Optimizer' artifact")
    print("   2. src/api/app.py - Copy from 'Fixed Enhanced Flask API' artifact")
    print("   3. scripts/test_enhanced_rag.py - Copy from 'Enhanced RAG System Test Script' artifact")
    
    print("\n🚀 Quick test commands:")
    print("   Test API: python src/api/app.py")
    print("   Test system: python scripts/test_enhanced_rag.py")
    
    return True

def create_simple_query_optimizer():
    """Create a simplified query optimizer for immediate testing"""
    query_optimizer_code = '''"""
Simple Query Optimizer for Enhanced RAG
"""

import os
import asyncio
from typing import List, Dict, Any
from openai import AsyncAzureOpenAI
from dotenv import load_dotenv

load_dotenv()

class AdvancedQueryOptimizer:
    def __init__(self):
        self.client = AsyncAzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.chat_model = os.getenv("AZURE_OPENAI_CHAT_MODEL", "gpt-4.1")
        
    async def optimize_query(self, user_query: str, conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """Optimize user query for better search results"""
        try:
            system_prompt = """You are a search query optimizer. Transform conversational questions into effective search terms.

RULES:
1. Extract key business terms
2. Add relevant synonyms
3. Remove conversational words
4. Add domain terms

EXAMPLES:
"What's our remote work policy?" → "remote work policy telecommuting work from home WFH guidelines procedures"
"How do expense approvals work?" → "expense approval process reimbursement procedures spending authority"

Return only the optimized search terms."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Optimize: {user_query}"}
            ]

            response = await self.client.chat.completions.create(
                model=self.chat_model,
                messages=messages,
                temperature=0.1,
                max_tokens=100
            )
            
            optimized_query = response.choices[0].message.content.strip()
            
            return {
                "optimized_query": optimized_query,
                "reasoning": "AI-optimized for better search relevance",
                "original_query": user_query
            }
            
        except Exception as e:
            # Fallback to basic optimization
            return {
                "optimized_query": self.basic_optimize(user_query),
                "reasoning": f"Basic optimization (AI failed: {str(e)})",
                "original_query": user_query
            }
    
    def basic_optimize(self, query: str) -> str:
        """Basic keyword optimization"""
        # Remove common words
        stop_words = ['what', 'how', 'is', 'our', 'the', 'can', 'you']
        words = [w for w in query.lower().split() if w not in stop_words]
        
        # Add domain terms
        expansions = {
            'remote': 'remote telecommuting work from home WFH',
            'expense': 'expense reimbursement spending cost',
            'policy': 'policy procedure guideline rule',
            'vacation': 'vacation PTO leave time off'
        }
        
        expanded = []
        for word in words:
            expanded.append(word)
            if word in expansions:
                expanded.extend(expansions[word].split())
        
        return ' '.join(expanded)

# Test function
async def test_optimizer():
    optimizer = AdvancedQueryOptimizer()
    result = await optimizer.optimize_query("What is our remote work policy?")
    print("Test result:", result)

if __name__ == "__main__":
    asyncio.run(test_optimizer())
'''
    
    with open("src/core/query_optimizer.py", "w") as f:
        f.write(query_optimizer_code)
    
    print("   ✅ Created: src/core/query_optimizer.py")
    return True

def create_minimal_enhanced_api():
    """Create a minimal enhanced API that should work immediately"""
    api_code = '''"""
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
            context = "\\n\\n".join([
                f"Source: {s.get('sourcefile', 'Unknown')}\\n{s.get('content', '')}"
                for s in sources[:3]
            ])
            
            messages = [
                {"role": "system", "content": f"Answer based on these sources. Include citations [filename].\\n\\nSources:\\n{context}"},
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
    print("🌐 Health: http://localhost:5001/api/health")
    app.run(debug=True, host='0.0.0.0', port=5001)
'''
    
    with open("src/api/app.py", "w") as f:
        f.write(api_code)
    
    print("   ✅ Created: src/api/app.py")
    return True

if __name__ == "__main__":
    print("🚀 QUICK SETUP FOR ENHANCED RAG")
    print("=" * 50)
    
    setup_enhanced_rag()
    create_simple_query_optimizer()
    create_minimal_enhanced_api()
    
    print("\n🎉 SETUP COMPLETE!")
    print("✅ Files created successfully")
    print("\n🔧 Test Commands:")
    print("   1. Start server: python src/api/app.py")
    print("   2. Test health: curl http://localhost:5001/api/health")
    print("   3. Test chat: curl -X POST http://localhost:5001/api/chat -H 'Content-Type: application/json' -d '{\"messages\":[{\"role\":\"user\",\"content\":\"What is our remote work policy?\"}]}'")