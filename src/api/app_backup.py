"""
Enhanced Flask API with proper environment loading
"""

import re
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

class EnhancedRAGEngineWithPriority:
    def __init__(self):
        # Initialize existing components
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
        
        # Priority document configuration
        self.priority_documents = {
            "employee_directory": {
                "filename_patterns": [
                    "magic meeting tracker",
                    "employee directory", 
                    "staff roster",
                    "team directory"
                ],
                "keywords": [
                    "employee", "staff", "team", "person", "who is", "who works",
                    "contact", "phone", "email", "manager", "department", "role",
                    "title", "position", "hudson", "autobahn", "consultant"
                ],
                "boost_factor": 15.0,  # High boost for employee queries
                "description": "Magic Meeting Tracker - Employee Directory"
            }
        }
        
        print("✅ Enhanced RAG Engine with Priority Documents initialized")
    
    def detect_query_category(self, query: str) -> str:
        """Detect if query is employee-related"""
        query_lower = query.lower()
        
        employee_keywords = [
            "employee", "staff", "team", "person", "who is", "who works",
            "contact", "phone", "email", "manager", "department", "role",
            "title", "position", "hudson", "autobahn", "consultant", "client"
        ]
        
        for keyword in employee_keywords:
            if keyword in query_lower:
                return "employee_directory"
        
        return None
    
    async def enhanced_search(self, query: str, overrides: Dict) -> Dict:
        """Enhanced search with Magic Meeting Tracker priority"""
        try:
            category = self.detect_query_category(query)
            top = overrides.get("top", 5)
            
            print(f"🔍 Search category detected: {category}")
            
            results = {"sources": [], "search_query": query, "category_detected": category}
            
            if category == "employee_directory":
                # Step 1: Search Magic Meeting Tracker first
                priority_results = await self._search_magic_meeting_tracker(query, top // 2 + 1)
                results["sources"].extend(priority_results)
                results["priority_search_used"] = True
                results["priority_document"] = "Magic Meeting Tracker"
                
                print(f"✅ Found {len(priority_results)} results from Magic Meeting Tracker")
            
            # Step 2: Fill remaining slots with general search
            remaining_slots = max(1, top - len(results["sources"]))
            if remaining_slots > 0:
                general_results = await self._general_search(query, remaining_slots)
                results["sources"].extend(general_results)
            
            results["total_sources"] = len(results["sources"])
            return results
            
        except Exception as e:
            print(f"Enhanced search error: {str(e)}")
            # Fallback to regular search
            return await self._general_search(query, overrides.get("top", 5))
    
    async def _search_magic_meeting_tracker(self, query: str, top: int) -> List[Dict]:
        """Search specifically in Magic Meeting Tracker"""
        try:
            # Search for documents containing "magic meeting tracker"
            filter_expression = "search.ismatch('magic meeting tracker', 'filename')"
            
            results = self.search_client.search(
                search_text=query,
                filter=filter_expression,
                top=top * 3,  # Get more results from this important document
                search_mode="any"
            )
            
            sources = []
            for result in results:
                if len(sources) >= top:
                    break
                
                # Apply high boost to Magic Meeting Tracker results
                original_score = float(result.get("@search.score", 0))
                boosted_score = original_score * 15.0  # 15x boost
                
                source = {
                    "content": result.get("chunk", ""),
                    "content_preview": result.get("chunk", "")[:200] + "..." if len(result.get("chunk", "")) > 200 else result.get("chunk", ""),
                    "sourcefile": result.get("filename", ""),
                    "sourcepage": result.get("document_path", ""),
                    "title": result.get("title", ""),
                    "chunk_id": result.get("chunk_id", ""),
                    "score": boosted_score,
                    "original_score": original_score,
                    "boost_applied": "Magic Meeting Tracker Priority (15x)",
                    "source_type": "priority_document"
                }
                sources.append(source)
            
            # Sort by boosted score
            sources.sort(key=lambda x: x["score"], reverse=True)
            return sources[:top]
            
        except Exception as e:
            print(f"Magic Meeting Tracker search error: {str(e)}")
            return []
    
    async def _general_search(self, query: str, top: int) -> List[Dict]:
        """Regular search for non-priority results"""
        try:
            results = self.search_client.search(
                search_text=query,
                top=top,
                search_mode="any"
            )
            
            sources = []
            for result in results:
                source = {
                    "content": result.get("chunk", ""),
                    "content_preview": result.get("chunk", "")[:200] + "..." if len(result.get("chunk", "")) > 200 else result.get("chunk", ""),
                    "sourcefile": result.get("filename", ""),
                    "sourcepage": result.get("document_path", ""),
                    "title": result.get("title", ""),
                    "chunk_id": result.get("chunk_id", ""),
                    "score": float(result.get("@search.score", 0)),
                    "source_type": "general_search"
                }
                sources.append(source)
            
            return sources
            
        except Exception as e:
            print(f"General search error: {str(e)}")
            return []
    
    async def enhanced_chat(self, messages: List[Dict], session_id: str = "default", overrides: Dict = None) -> Dict:
        """Enhanced chat with Magic Meeting Tracker priority"""
        thoughts = []
        start_time = datetime.utcnow()
        
        try:
            user_query = messages[-1]["content"]
            conversation_history = conversation_memory.get(session_id, [])
            
            # Step 1: Query Optimization
            thoughts.append({
                "title": "Query Optimization",
                "description": "Analyzing query for employee directory priority",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            optimization_result = await self.query_optimizer.optimize_query(user_query, conversation_history)
            optimized_query = optimization_result["optimized_query"]
            
            # Detect if employee-related
            category = self.detect_query_category(user_query)
            
            thoughts.append({
                "title": "Query Enhanced", 
                "description": f"'{user_query}' → '{optimized_query}'",
                "details": f"Category: {category or 'general'}" + (", Magic Meeting Tracker priority enabled" if category else ""),
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Step 2: Enhanced Search with Priority
            thoughts.append({
                "title": "Document Search",
                "description": "Searching with Magic Meeting Tracker priority" if category else "General search",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            search_results = await self.enhanced_search(optimized_query, overrides or {})
            
            priority_note = ""
            if search_results.get("priority_search_used"):
                priority_note = f" (Magic Meeting Tracker prioritized)"
            
            thoughts.append({
                "title": "Sources Retrieved",
                "description": f"Found {len(search_results['sources'])} relevant documents{priority_note}",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Step 3: Response Generation
            thoughts.append({
                "title": "Answer Generation", 
                "description": "Creating comprehensive response with citations",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            response = await self.generate_response(user_query, search_results["sources"])
            
            # Step 4: Generate Follow-up Questions
            follow_up_questions = self.generate_smart_follow_up_questions(user_query, category)
            
            # Update conversation memory
            conversation_memory[session_id] = conversation_history + [
                {"role": "user", "content": user_query},
                {"role": "assistant", "content": response["content"]}
            ][-20:]
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            return {
                "message": response,
                "context": {
                    "thoughts": thoughts,
                    "data_points": search_results["sources"],
                    "followup_questions": follow_up_questions,
                    "optimization": optimization_result,
                    "priority_search": search_results.get("priority_search_used", False),
                    "category_detected": category,
                    "session_id": session_id,
                    "processing_time": f"{processing_time:.2f}s"
                }
            }
            
        except Exception as e:
            return {
                "message": {"content": f"Error: {str(e)}", "role": "assistant"},
                "context": {"thoughts": thoughts, "error": str(e)}
            }
    
    async def generate_response(self, user_query: str, sources: List[Dict]) -> Dict:
        """Generate response with Magic Meeting Tracker awareness"""
        try:
            # Build context highlighting Magic Meeting Tracker content
            context_text = ""
            magic_tracker_sources = 0
            
            for i, source in enumerate(sources[:5]):
                content = source.get("content", "")
                filename = source.get("sourcefile", "")
                
                # Highlight Magic Meeting Tracker sources
                if "magic meeting tracker" in filename.lower():
                    magic_tracker_sources += 1
                    context_text += f"\n\n🔥 PRIORITY SOURCE {i+1} - Magic Meeting Tracker ({filename}):\n{content}"
                else:
                    context_text += f"\n\nSource {i+1} ({filename}):\n{content}"
            
            system_prompt = f"""You are a helpful AI assistant with access to comprehensive employee and client information from Autobahn Consultants.

IMPORTANT: The Magic Meeting Tracker (7/30/25) contains the most up-to-date and comprehensive employee, client, and meeting information. When information from this document is available, prioritize it over other sources.

Your role is to provide accurate, helpful answers about:
- Employee information and organizational structure
- Client relationships and project details  
- Meeting records and action items
- Contact information and team details

RESPONSE GUIDELINES:
1. Prioritize information from Magic Meeting Tracker when available
2. Include specific citations [filename] for key claims
3. Be comprehensive but concise
4. If the query is about a specific person, provide their role, contact info, and relevant details
5. For client-related queries, include project context when available

Magic Meeting Tracker sources found: {magic_tracker_sources}

SOURCE DOCUMENTS:{context_text}

Answer the user's question comprehensively using the provided sources, with special attention to Magic Meeting Tracker information."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ]
            
            response = await self.openai_client.chat.completions.create(
                model=self.chat_model,
                messages=messages,
                temperature=0.3,
                max_tokens=800
            )
            
            return {"content": response.choices[0].message.content, "role": "assistant"}
            
        except Exception as e:
            return {"content": f"Found relevant information but couldn't generate response: {str(e)}", "role": "assistant"}
    
    def generate_smart_follow_up_questions(self, user_query: str, category: str) -> List[str]:
        """Generate contextual follow-up questions"""
        if category == "employee_directory":
            return [
                "What projects is this person currently working on?",
                "Who else is on their team?",
                "What are their contact details?"
            ]
        else:
            return [
                f"Can you provide more details about {user_query.lower()}?",
                "Are there related policies I should know about?",
                "What are the next steps?"
            ]

try:
    rag_engine = EnhancedRAGEngineWithPriority()
    print("🎉 Enhanced RAG engine with Magic Meeting Tracker priority ready!")
except Exception as e:
    print(f"💥 Failed to initialize RAG engine: {str(e)}")
    exit(1)

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

# Add these routes to your src/api/app.py file, right before the if __name__ == '__main__': line

@app.route('/api/chat/sessions/<session_id>/history', methods=['GET'])
def get_conversation_history(session_id):
    """Get conversation history for a session"""
    history = conversation_memory.get(session_id, [])
    return jsonify({
        "session_id": session_id,
        "message_count": len(history),
        "messages": history
    })

@app.route('/api/chat/sessions/<session_id>/clear', methods=['POST'])
def clear_conversation_history(session_id):
    """Clear conversation history for a session"""
    if session_id in conversation_memory:
        del conversation_memory[session_id]
        return jsonify({"message": f"Cleared conversation history for session {session_id}"})
    else:
        return jsonify({"message": f"No conversation history found for session {session_id}"})

@app.route('/api/search', methods=['POST'])
def search():
    """Direct search endpoint"""
    try:
        data = request.get_json()
        query = data.get("query", "")
        overrides = data.get("overrides", {})
        
        if not query:
            return jsonify({"error": "Missing 'query' parameter"}), 400
        
        # Run async search
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(rag_engine.search_documents(query, overrides))
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