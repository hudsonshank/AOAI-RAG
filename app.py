#!/usr/bin/env python3
"""
Production Flask server for Jennifur RAG system on Azure App Service
"""

import os
import sys
from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
import logging

# Configure logging for Azure App Service
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import the RAG engine
try:
    from api.client_aware_rag import ClientAwareRAGEngine
    logger.info("Successfully imported ClientAwareRAGEngine")
except ImportError as e:
    logger.error(f"Failed to import RAG engine: {e}")
    ClientAwareRAGEngine = None

# Create Flask app
app = Flask(__name__)
CORS(app, origins="*", supports_credentials=True)

# Initialize RAG engine globally
rag_engine = None

def initialize_rag():
    """Initialize the RAG engine"""
    global rag_engine
    try:
        if ClientAwareRAGEngine is None:
            logger.error("ClientAwareRAGEngine not available")
            return False
            
        logger.info("Initializing RAG engine...")
        rag_engine = ClientAwareRAGEngine()
        logger.info("RAG engine initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize RAG engine: {str(e)}")
        return False

@app.route('/', methods=['GET'])
def home():
    """Home endpoint"""
    return jsonify({
        "service": "Jennifur RAG API",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "chat": "/api/chat"
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "rag_initialized": rag_engine is not None,
        "environment": "production"
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
                    session_id=data.get('session_id', 'production-session')
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

# Initialize RAG engine on startup
try:
    initialize_rag()
except Exception as e:
    logger.error(f"Failed to initialize RAG on startup: {e}")

if __name__ == '__main__':
    # For local testing
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=False)