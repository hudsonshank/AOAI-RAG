"""
Enhanced Flask API with Client-Aware RAG Engine
Updated to use the client metadata tagging system
"""

import re
import os
import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment first
load_dotenv()

# Import the client-aware RAG engine
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from api.client_aware_rag import ClientAwareRAGEngine

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend integration
conversation_memory = {}

# Initialize the client-aware RAG engine
try:
    rag_engine = ClientAwareRAGEngine()
    print("🎉 Client-Aware RAG engine ready!")
    print("✅ Features: client filtering, PM filtering, category filtering")
    
    # Get available clients for reference
    clients = rag_engine.get_client_list()
    if clients:
        print(f"🏢 Available clients: {len(clients)} total")
        for client in clients[:5]:  # Show top 5
            print(f"   - {client['name']}: {client['document_count']} documents")
        if len(clients) > 5:
            print(f"   ... and {len(clients) - 5} more clients")
    
except Exception as e:
    print(f"💥 Failed to initialize Client-Aware RAG engine: {str(e)}")
    exit(1)

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "version": "client-aware-v2.0",
        "features": [
            "client_filtering", 
            "pm_filtering", 
            "category_filtering",
            "query_optimization", 
            "conversation_memory", 
            "thought_transparency"
        ]
    })

@app.route('/api/chat', methods=['POST'])
def chat():
    """Enhanced chat endpoint with client awareness"""
    try:
        data = request.get_json()
        messages = data.get("messages", [])
        session_id = data.get("session_id", "default")
        
        # Client filtering parameters
        client_context = data.get("client_context")  # Specific client to focus on
        pm_context = data.get("pm_context")  # PM initial (C, S, K)
        
        if not messages:
            return jsonify({"error": "Missing 'messages' parameter"}), 400
        
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                rag_engine.client_aware_chat(
                    messages=messages,
                    client_context=client_context,
                    pm_context=pm_context,
                    session_id=session_id
                )
            )
        finally:
            loop.close()
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Chat error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/search', methods=['POST'])
def search():
    """Enhanced search endpoint with client filtering"""
    try:
        data = request.get_json()
        query = data.get("query", "")
        
        if not query:
            return jsonify({"error": "Missing 'query' parameter"}), 400
        
        # Client filtering parameters
        client_name = data.get("client_name")
        pm_initial = data.get("pm_initial") 
        document_category = data.get("document_category")
        top = data.get("top", 5)
        include_internal = data.get("include_internal", True)
        
        # Run async search
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                rag_engine.client_aware_search(
                    query=query,
                    client_name=client_name,
                    pm_initial=pm_initial,
                    document_category=document_category,
                    top=top,
                    include_internal=include_internal
                )
            )
        finally:
            loop.close()
            
        return jsonify(result)
        
    except Exception as e:
        print(f"Search error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/clients', methods=['GET'])
def get_clients():
    """Get available clients in the system"""
    try:
        clients = rag_engine.get_client_list()
        
        return jsonify({
            "clients": clients,
            "total_clients": len(clients)
        })
        
    except Exception as e:
        print(f"Get clients error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/clients/<client_name>/stats', methods=['GET'])
def get_client_stats(client_name):
    """Get statistics for a specific client"""
    try:
        # Search for documents from this client
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                rag_engine.client_aware_search(
                    query="*",
                    client_name=client_name,
                    top=100,
                    include_internal=False
                )
            )
        finally:
            loop.close()
        
        # Group by category
        categories = {}
        for source in result["sources"]:
            category = source.get("document_category", "general")
            categories[category] = categories.get(category, 0) + 1
        
        return jsonify({
            "client_name": client_name,
            "total_documents": len(result["sources"]),
            "categories": categories
        })
        
    except Exception as e:
        print(f"Client stats error: {str(e)}")
        return jsonify({"error": str(e)}), 500

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

@app.route('/api/search/suggestions', methods=['POST'])
def get_search_suggestions():
    """Get search suggestions based on client context"""
    try:
        data = request.get_json()
        client_name = data.get("client_name")
        pm_initial = data.get("pm_initial")
        
        suggestions = []
        
        if client_name:
            suggestions.extend([
                f"What financial reports does {client_name} have?",
                f"Show me {client_name} meeting notes",
                f"What projects is {client_name} working on?",
                f"{client_name} behavioral profiles",
            ])
        elif pm_initial:
            suggestions.extend([
                f"What clients does PM-{pm_initial} manage?",
                f"Show me PM-{pm_initial} project updates",
                f"PM-{pm_initial} client financials",
            ])
        else:
            suggestions.extend([
                "What clients do we work with?",
                "Show me recent meeting notes", 
                "What training materials are available?",
                "Autobahn tools and resources"
            ])
        
        return jsonify({
            "suggestions": suggestions[:6]  # Limit to 6 suggestions
        })
        
    except Exception as e:
        print(f"Suggestions error: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Backward compatibility route (for old frontend)
@app.route('/api/chat_old', methods=['POST'])
def chat_old():
    """Backward compatible chat endpoint (without client awareness)"""
    try:
        data = request.get_json()
        messages = data.get("messages", [])
        session_id = data.get("session_id", "default")
        
        if not messages:
            return jsonify({"error": "Missing 'messages' parameter"}), 400
        
        # Use client-aware chat without specific client context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                rag_engine.client_aware_chat(
                    messages=messages,
                    client_context=None,  # No specific client
                    pm_context=None,      # No specific PM
                    session_id=session_id
                )
            )
        finally:
            loop.close()
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Old chat error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("\n🚀 Client-Aware RAG API Server Starting...")
    print("✅ Environment loaded")
    print("🔍 Client filtering enabled")
    print("👥 PM filtering enabled") 
    print("📂 Category filtering enabled")
    print("\n🌐 API Endpoints:")
    print("   Health:     http://localhost:5001/api/health")
    print("   Chat:       POST http://localhost:5001/api/chat")
    print("   Search:     POST http://localhost:5001/api/search")
    print("   Clients:    GET  http://localhost:5001/api/clients")
    print("   Suggestions: POST http://localhost:5001/api/search/suggestions")
    print("\n📋 New Features:")
    print("   • Auto-detect clients from queries")
    print("   • Filter by client_name, pm_initial, document_category") 
    print("   • Client-aware response generation")
    print("   • Enhanced search with metadata filtering")
    
    app.run(debug=True, host='0.0.0.0', port=5001)