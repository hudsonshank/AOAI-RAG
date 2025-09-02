#!/usr/bin/env python3
"""
Local Flask server for Jennifur RAG system
Run this to test the RAG system locally without Azure Functions
"""

import os
import sys
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import asyncio
import logging

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import the RAG engine
from api.client_aware_rag import ClientAwareRAGEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
CORS(app, origins=["http://localhost:3000", "http://localhost:5000", "http://127.0.0.1:5000", "file://"])

# Initialize RAG engine globally
rag_engine = None

def initialize_rag():
    """Initialize the RAG engine"""
    global rag_engine
    try:
        logger.info("Initializing RAG engine...")
        rag_engine = ClientAwareRAGEngine()
        logger.info("RAG engine initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize RAG engine: {str(e)}")
        return False

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "rag_initialized": rag_engine is not None
    })

@app.route('/api/chat', methods=['POST', 'OPTIONS'])
def chat():
    """Chat endpoint that mimics Azure Function behavior"""
    
    # Handle CORS preflight
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body is required"}), 400
        
        messages = data.get('messages', [])
        if not messages:
            return jsonify({"error": "Messages array is required"}), 400
        
        # Initialize RAG if needed
        if rag_engine is None:
            if not initialize_rag():
                return jsonify({"error": "Failed to initialize RAG engine"}), 500
        
        # Process chat
        logger.info(f"Processing chat with {len(messages)} messages")
        
        # Run async function in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            response = loop.run_until_complete(
                rag_engine.client_aware_chat(
                    messages=messages,
                    client_context=data.get("client_context"),
                    pm_context=data.get("pm_context"),
                    session_id=data.get('session_id', 'local-session')
                )
            )
            logger.info("Successfully generated RAG response")
            return jsonify(response)
        except Exception as e:
            logger.error(f"Chat processing failed: {str(e)}")
            return jsonify({"error": f"Chat processing failed: {str(e)}"}), 500
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

if __name__ == '__main__':
    # Initialize RAG engine on startup
    initialize_rag()
    
    # Get port from environment or use default
    port = int(os.getenv('API_PORT', 5001))
    
    print(f"""
    ╔══════════════════════════════════════════════╗
    ║     Jennifur RAG Local Server Starting       ║
    ╠══════════════════════════════════════════════╣
    ║  Server running at: http://localhost:{port}    ║
    ║  Health check: http://localhost:{port}/health ║
    ║  Chat API: http://localhost:{port}/api/chat   ║
    ╠══════════════════════════════════════════════╣
    ║  Press Ctrl+C to stop the server             ║
    ╚══════════════════════════════════════════════╝
    """)
    
    # Run the Flask app
    app.run(
        host='0.0.0.0',
        port=port,
        debug=os.getenv('FLASK_DEBUG', 'true').lower() == 'true'
    )