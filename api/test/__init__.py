import json
import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Simple test function to verify Azure Functions are working
    """
    logging.info('Test function triggered')
    
    # Set CORS headers
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }
    
    # Handle preflight OPTIONS request
    if req.method == 'OPTIONS':
        return func.HttpResponse(
            "",
            status_code=200,
            headers=headers
        )
    
    try:
        # Simple test response
        response = {
            "message": "Azure Function is working!",
            "method": req.method,
            "url": req.url,
            "headers": dict(req.headers),
            "status": "success"
        }
        
        return func.HttpResponse(
            json.dumps(response, indent=2),
            status_code=200,
            mimetype="application/json",
            headers=headers
        )
        
    except Exception as e:
        logging.error(f'Error in test function: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers=headers
        )