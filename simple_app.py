"""
Simple Flask API wrapper for the RAG engine
Minimal deployment for Azure App Service
"""

import os
import sys
import asyncio
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the client-aware RAG engine
from api.client_aware_rag import ClientAwareRAGEngine

app = Flask(__name__)
CORS(app, origins=["https://jennifur.autobahnconsultants.ai", "https://victorious-ocean-0d7acf60f.1.azurestaticapps.net", "http://localhost:*"])

# Initialize RAG engine
try:
    rag_engine = ClientAwareRAGEngine()
    print("✅ RAG engine initialized successfully")
except Exception as e:
    print(f"❌ Failed to initialize RAG engine: {e}")
    rag_engine = None

@app.route('/')
def home():
    return jsonify({"status": "Jennifur RAG API is running", "version": "2.0"})

@app.route('/api/health')
def health():
    return jsonify({
        "status": "healthy" if rag_engine else "unhealthy",
        "version": "2.0",
        "engine": "initialized" if rag_engine else "failed"
    })

@app.route('/api/chat', methods=['POST'])
def chat():
    if not rag_engine:
        return jsonify({"error": "RAG engine not initialized"}), 500
    
    try:
        data = request.get_json()
        messages = data.get("messages", [])
        session_id = data.get("session_id", "default")
        client_context = data.get("client_context")
        pm_context = data.get("pm_context")
        
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
        print(f"Chat error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port)