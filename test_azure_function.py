#!/usr/bin/env python3
"""
Test script for Azure Function - Enhanced Excel Processing
Tests the deployed jennifur-document-ingestion function with Excel files
"""

import requests
import json
import os
import sys
from pathlib import Path

def test_azure_function_with_excel(excel_file_path=None, doc_path=None):
    """Test the Azure Function with an Excel file"""
    
    # Azure Function endpoint
    function_url = "https://jennifur-document-ingestion.azurewebsites.net/api/process_single_document"
    
    print("🔍 Testing Azure Function - Enhanced Excel Processing")
    print("=" * 60)
    print(f"Function URL: {function_url}")
    
    # If no file path provided, prompt for one
    if not excel_file_path:
        print("\nLooking for Excel files in common locations...")
        potential_files = [
            "contact_directory.xlsx",
            "client_contacts.xlsx", 
            "executive_team.xlsx",
            "Book2.xlsx",  # From your Downloads
        ]
        
        found_file = None
        for potential_file in potential_files:
            if os.path.exists(potential_file):
                found_file = potential_file
                break
        
        if found_file:
            excel_file_path = found_file
            print(f"Found Excel file: {excel_file_path}")
        else:
            print("No Excel files found. Please provide the path to your contact directory Excel file:")
            excel_file_path = input("Excel file path: ").strip()
    
    # Validate file exists
    if not excel_file_path or not os.path.exists(excel_file_path):
        print(f"❌ File not found: {excel_file_path}")
        return
    
    # Set up document path (SharePoint-style path for client metadata extraction)
    if not doc_path:
        # Extract client info from filename or use default pattern
        filename = os.path.basename(excel_file_path)
        doc_path = f"/ClientName (PM-S)/{filename}"  # Example pattern
        print(f"Using document path: {doc_path}")
        
        # Ask if user wants to customize the path
        custom_path = input(f"Enter custom SharePoint path (or press Enter to use '{doc_path}'): ").strip()
        if custom_path:
            doc_path = custom_path
    
    print(f"📊 Processing Excel file: {excel_file_path}")
    print(f"📁 Document path: {doc_path}")
    
    # Prepare the request payload
    payload = {
        "doc_path": doc_path,
        "process_type": "single_document"
    }
    
    try:
        # Make the request
        print("\n🚀 Sending request to Azure Function...")
        response = requests.post(
            function_url,
            json=payload,
            headers={
                'Content-Type': 'application/json'
            },
            timeout=300  # 5 minute timeout for processing
        )
        
        print(f"Response Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("\n✅ SUCCESS! Function executed successfully")
            print("-" * 40)
            
            # Display key results
            print(f"Action: {result.get('action', 'N/A')}")
            print(f"Processing Method: {result.get('processing_method', 'N/A')}")
            print(f"Chunks Created: {result.get('chunks_stored', 'N/A')}")
            print(f"Content Length: {result.get('content_length', 'N/A')} characters")
            
            # Show Excel-specific information
            if 'excel_sheets_processed' in result:
                print(f"Excel Sheets Processed: {result['excel_sheets_processed']}")
                print(f"Sheet Names: {', '.join(result.get('sheet_names', []))}")
            
            # Show client information if detected
            if 'client_name' in result:
                print(f"Client Detected: {result['client_name']}")
                print(f"PM: {result.get('pm_name', 'N/A')}")
            
            print(f"Processing Time: {result.get('processing_time', 'N/A')}")
            
            # Save full result for inspection
            output_file = f"azure_function_result_{os.path.basename(excel_file_path)}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"\n💾 Full result saved to: {output_file}")
            
        else:
            print(f"\n❌ ERROR! Function returned status {response.status_code}")
            print("Response:", response.text)
            
            # Try to parse error details
            try:
                error_data = response.json()
                print("Error details:", json.dumps(error_data, indent=2))
            except:
                pass
    
    except requests.exceptions.Timeout:
        print("⏰ Request timed out. The file might be very large or the function is busy.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {str(e)}")
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")

def test_function_status():
    """Test if the Azure Function is responding"""
    print("🏥 Testing Azure Function Health")
    print("-" * 40)
    
    try:
        # Test with a simple request to see if function is responding
        test_payload = {"test": "ping"}
        response = requests.post(
            "https://jennifur-document-ingestion.azurewebsites.net/api/process_single_document",
            json=test_payload,
            timeout=30
        )
        
        print(f"Function Status: {response.status_code}")
        if response.status_code in [200, 400, 422]:  # Any response means it's alive
            print("✅ Function is responding")
        else:
            print("⚠️  Function responded but with unexpected status")
            
    except Exception as e:
        print(f"❌ Function appears to be down: {str(e)}")

if __name__ == "__main__":
    print("Azure Function Test Options:")
    print("1. Test function health")
    print("2. Process Excel file")
    
    choice = input("\nSelect option (1 or 2): ").strip()
    
    if choice == "1":
        test_function_status()
    elif choice == "2":
        # Check for command line arguments
        excel_file = sys.argv[1] if len(sys.argv) > 1 else None
        doc_path = sys.argv[2] if len(sys.argv) > 2 else None
        test_azure_function_with_excel(excel_file, doc_path)
    else:
        print("Invalid choice. Running Excel processing test...")
        test_azure_function_with_excel()