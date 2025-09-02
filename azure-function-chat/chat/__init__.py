import json
import logging
import azure.functions as func
import os
import sys

# Import the RAG engine directly - try parent directory first
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from client_aware_rag import ClientAwareRAGEngine
    import_success = True
except ImportError as e:
    import_success = False
    import_error = str(e)

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function for RAG chat endpoint
    """
    logging.info('RAG chat function triggered')
    
    # Set CORS headers
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }
    
    # Handle preflight OPTIONS request
    if req.method == 'OPTIONS':
        return func.HttpResponse(
            "",
            status_code=200,
            headers=headers
        )
    
    # Check if import was successful
    if not import_success:
        return func.HttpResponse(
            json.dumps({
                "error": f"Failed to import RAG engine: {import_error}",
                "sys_path": sys.path[:3],
                "parent_dir": parent_dir
            }),
            status_code=500,
            mimetype="application/json",
            headers=headers
        )
    
    try:
        logging.info('Processing RAG chat request')
        req_body = req.get_json()
        
        if not req_body:
            logging.warning('No request body provided')
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json",
                headers=headers
            )
        
        messages = req_body.get('messages', [])
        session_id = req_body.get('session_id', 'default')
        
        if not messages:
            return func.HttpResponse(
                json.dumps({"error": "Messages array is required"}),
                status_code=400,
                mimetype="application/json",
                headers=headers
            )
        
        # Get the last user message
        user_message = None
        for msg in reversed(messages):
            if msg.get('role') == 'user':
                user_message = msg.get('content', '')
                break
        
        if not user_message:
            return func.HttpResponse(
                json.dumps({"error": "No user message found"}),
                status_code=400,
                mimetype="application/json",
                headers=headers
            )
        
        # Initialize Jennifur RAG system
        logging.info('Initializing ClientAwareRAGEngine')
        try:
            rag_engine = ClientAwareRAGEngine()
            logging.info('RAG engine initialized successfully')
        except Exception as init_error:
            logging.error(f'Failed to initialize RAG engine: {str(init_error)}')
            return func.HttpResponse(
                json.dumps({"error": f"RAG engine initialization failed: {str(init_error)}"}),
                status_code=500,
                mimetype="application/json",
                headers=headers
            )
        
        # Get response from Jennifur
        logging.info(f'Processing chat with {len(messages)} messages')
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            response = loop.run_until_complete(
                rag_engine.client_aware_chat(
                    messages=messages,
                    client_context=req_body.get("client_context"),
                    pm_context=req_body.get("pm_context"),
                    session_id=session_id
                )
            )
            logging.info('Successfully generated RAG response')
        except Exception as chat_error:
            logging.error(f'Chat processing failed: {str(chat_error)}')
            return func.HttpResponse(
                json.dumps({"error": f"Chat processing failed: {str(chat_error)}"}),
                status_code=500,
                mimetype="application/json",
                headers=headers
            )
        finally:
            loop.close()
        
        return func.HttpResponse(
            json.dumps(response, indent=2),
            status_code=200,
            mimetype="application/json",
            headers=headers
        )
        
    except Exception as e:
        logging.error(f'Error in RAG chat function: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": f"Internal server error: {str(e)}"}),
            status_code=500,
            mimetype="application/json",
            headers=headers
        )