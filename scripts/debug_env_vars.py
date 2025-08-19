#!/usr/bin/env python3
"""
Debug Environment Variables Script
Check if all required environment variables are properly loaded
"""

import os
from dotenv import load_dotenv

def debug_environment():
    """Debug environment variable loading"""
    print("🔍 ENVIRONMENT VARIABLES DEBUG")
    print("=" * 60)
    
    # Load .env file
    print("1. Loading .env file...")
    env_file_path = ".env"
    
    if os.path.exists(env_file_path):
        print(f"   ✅ Found .env file at: {os.path.abspath(env_file_path)}")
        load_dotenv(env_file_path)
        print("   ✅ .env file loaded")
    else:
        print(f"   ❌ .env file not found at: {os.path.abspath(env_file_path)}")
        return False
    
    # Check required variables
    print("\n2. Checking required environment variables...")
    
    required_vars = {
        "AZURE_SEARCH_SERVICE_NAME": "Azure Search service name",
        "AZURE_SEARCH_ADMIN_KEY": "Azure Search admin key", 
        "AZURE_SEARCH_ENDPOINT": "Azure Search endpoint URL",
        "AZURE_OPENAI_API_KEY": "Azure OpenAI API key",
        "AZURE_OPENAI_ENDPOINT": "Azure OpenAI endpoint URL",
        "AZURE_OPENAI_CHAT_MODEL": "Chat model name (should be gpt-4.1)",
        "AZURE_OPENAI_EMBEDDING_MODEL": "Embedding model name",
        "AZURE_OPENAI_API_VERSION": "API version",
        "EXISTING_INDEX_NAME": "Search index name"
    }
    
    missing_vars = []
    empty_vars = []
    
    for var_name, description in required_vars.items():
        value = os.getenv(var_name)
        
        if value is None:
            missing_vars.append(var_name)
            print(f"   ❌ {var_name}: NOT SET ({description})")
        elif value.strip() == "":
            empty_vars.append(var_name)
            print(f"   ⚠️  {var_name}: EMPTY ({description})")
        else:
            # Mask sensitive values
            if "KEY" in var_name:
                masked_value = value[:8] + "..." + value[-4:] if len(value) > 12 else "***"
                print(f"   ✅ {var_name}: {masked_value} ({description})")
            else:
                print(f"   ✅ {var_name}: {value} ({description})")
    
    # Summary
    print(f"\n3. Summary:")
    print(f"   Total variables: {len(required_vars)}")
    print(f"   Missing: {len(missing_vars)}")
    print(f"   Empty: {len(empty_vars)}")
    print(f"   Valid: {len(required_vars) - len(missing_vars) - len(empty_vars)}")
    
    if missing_vars:
        print(f"\n❌ Missing variables: {', '.join(missing_vars)}")
    
    if empty_vars:
        print(f"\n⚠️  Empty variables: {', '.join(empty_vars)}")
    
    return len(missing_vars) == 0 and len(empty_vars) == 0

def show_env_file_contents():
    """Show .env file contents (masked)"""
    print("\n4. .env file contents (sensitive values masked):")
    print("-" * 60)
    
    try:
        with open(".env", "r") as f:
            lines = f.readlines()
            
        for i, line in enumerate(lines, 1):
            line = line.strip()
            if line and not line.startswith("#"):
                if "=" in line:
                    key, value = line.split("=", 1)
                    if "KEY" in key.upper():
                        masked_value = value[:8] + "..." + value[-4:] if len(value) > 12 else "***"
                        print(f"   {i:2d}. {key}={masked_value}")
                    else:
                        print(f"   {i:2d}. {key}={value}")
                else:
                    print(f"   {i:2d}. {line}")
            elif line.startswith("#"):
                print(f"   {i:2d}. {line}")
            else:
                print(f"   {i:2d}. (empty line)")
                
    except FileNotFoundError:
        print("   ❌ .env file not found")
    except Exception as e:
        print(f"   ❌ Error reading .env file: {str(e)}")

def test_azure_credentials():
    """Test if we can create Azure clients with the credentials"""
    print("\n5. Testing Azure credential validity...")
    
    try:
        from azure.search.documents import SearchClient
        from azure.core.credentials import AzureKeyCredential
        from openai import AsyncAzureOpenAI
        
        # Test Search credentials
        search_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
        search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        index_name = os.getenv("EXISTING_INDEX_NAME")
        
        if search_key and search_endpoint and index_name:
            try:
                search_client = SearchClient(
                    endpoint=search_endpoint,
                    index_name=index_name,
                    credential=AzureKeyCredential(search_key)
                )
                print("   ✅ Azure Search client created successfully")
            except Exception as e:
                print(f"   ❌ Azure Search client failed: {str(e)}")
        else:
            print("   ⚠️  Missing Azure Search credentials")
        
        # Test OpenAI credentials
        openai_key = os.getenv("AZURE_OPENAI_API_KEY")
        openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        
        if openai_key and openai_endpoint and api_version:
            try:
                openai_client = AsyncAzureOpenAI(
                    api_key=openai_key,
                    api_version=api_version,
                    azure_endpoint=openai_endpoint
                )
                print("   ✅ Azure OpenAI client created successfully")
            except Exception as e:
                print(f"   ❌ Azure OpenAI client failed: {str(e)}")
        else:
            print("   ⚠️  Missing Azure OpenAI credentials")
            
    except ImportError as e:
        print(f"   ❌ Import error: {str(e)}")
        print("   💡 Run: pip install azure-search-documents openai")

if __name__ == "__main__":
    print("Starting environment debug...")
    
    is_valid = debug_environment()
    show_env_file_contents()
    test_azure_credentials()
    
    print("\n" + "=" * 60)
    print("💡 RECOMMENDATIONS:")
    print("=" * 60)
    
    if not is_valid:
        print("1. Check your .env file format - each line should be: VARIABLE_NAME=value")
        print("2. Ensure no spaces around the = sign")
        print("3. Remove any quotes around values unless they're part of the actual value")
        print("4. Check for any special characters that might need escaping")
        print("5. Verify the .env file is in the project root directory")
    else:
        print("✅ Environment variables look good!")
        print("   Try running the Flask app again: python src/api/app.py")
    
    print("\n🔧 Quick Fix Commands:")
    print("   Check .env location: ls -la .env")
    print("   Load environment manually: source .env")
    print("   Test specific variable: echo $AZURE_SEARCH_ADMIN_KEY")