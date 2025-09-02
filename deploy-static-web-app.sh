#!/bin/bash

# Jennifur Static Web App Deployment Guide
echo "🚀 Deploying Jennifur to Azure Static Web App..."
echo ""
echo "DEPLOYMENT STEPS:"
echo ""

echo "1. 📁 Prepare deployment structure:"
echo "   Your project structure should be:"
echo "   ├── frontend/"
echo "   │   └── index.html        (your frontend)"
echo "   ├── api/"  
echo "   │   ├── host.json"
echo "   │   ├── requirements.txt"
echo "   │   └── chat/"
echo "   │       ├── __init__.py   (Azure Functions API)"
echo "   │       └── function.json"
echo "   ├── src/                  (your RAG code)"
echo "   └── staticwebapp.config.json"
echo ""

echo "2. 🔧 Get your Static Web App deployment token:"
echo "   az staticwebapp secrets list --name jennifur --query 'properties.apiKey' -o tsv"
echo ""

# Get the deployment token
echo "📋 Getting deployment token..."
DEPLOYMENT_TOKEN=$(az staticwebapp secrets list --name jennifur --query 'properties.apiKey' -o tsv 2>/dev/null)

if [ -z "$DEPLOYMENT_TOKEN" ]; then
    echo "❌ Could not get deployment token. Please run manually:"
    echo "   az staticwebapp secrets list --name jennifur --query 'properties.apiKey' -o tsv"
    echo ""
    echo "3. 🚀 Then deploy with:"
    echo "   npx @azure/static-web-apps-cli deploy \\"
    echo "     --deployment-token [YOUR_TOKEN] \\"
    echo "     --app-location ./frontend \\"
    echo "     --api-location ./api \\"
    echo "     --output-location ./"
else
    echo "✅ Found deployment token!"
    echo ""
    echo "3. 🚀 Deploying to Static Web App..."
    
    # Check if SWA CLI is installed
    if ! command -v swa &> /dev/null; then
        echo "📦 Installing Azure Static Web Apps CLI..."
        npm install -g @azure/static-web-apps-cli
    fi
    
    # Deploy
    npx @azure/static-web-apps-cli deploy \
        --deployment-token "$DEPLOYMENT_TOKEN" \
        --app-location ./frontend \
        --api-location ./api \
        --output-location ./
        
    echo ""
    echo "✅ Deployment initiated!"
fi

echo ""
echo "4. 🌐 Your Static Web App will be available at:"
echo "   https://[your-static-web-app-name].azurestaticapps.net"
echo ""
echo "5. ⚙️ Configure environment variables in Azure Portal:"
echo "   - AZURE_SEARCH_SERVICE_NAME"
echo "   - AZURE_SEARCH_INDEX_NAME" 
echo "   - AZURE_OPENAI_ENDPOINT"
echo "   - AZURE_OPENAI_DEPLOYMENT_NAME"
echo "   - AZURE_KEY_VAULT_URL"
echo ""
echo "6. 🧪 Test your deployment:"
echo "   - Visit your Static Web App URL"
echo "   - Try sending a chat message"
echo "   - Check browser dev tools for any API errors"