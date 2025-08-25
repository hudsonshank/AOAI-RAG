"""
Client Metadata Extraction Utility for Autobahn Consultants
Extracts client information from SharePoint folder structure patterns
"""

import re
import logging
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass
from pathlib import Path

@dataclass
class ClientInfo:
    """Client information extracted from document path"""
    client_name: str
    pm_initial: str
    pm_name: str
    folder_path: str
    is_client_specific: bool
    document_category: str

class ClientMetadataExtractor:
    """Extract client metadata from Autobahn Consultants SharePoint paths"""
    
    def __init__(self):
        # Pattern for client folders: /ClientName (PM-X)/
        self.client_pattern = re.compile(r'/([^/]+) \(PM-([A-Z])\)/')
        
        # PM name mappings
        self.pm_names = {
            'C': 'Caleb',
            'S': 'Sam', 
            'K': 'Katherine'
        }
        
        # Internal/tool folders (non-client specific)
        self.internal_folders = {
            'autobahn tools',
            'autobook and articles', 
            'pm & apm training materials',
            'archive [do not use]',
            'rock the recession',
            'to sort',
            'parking lot',
            'jennifur',
            'general resources'
        }
        
        # Document category mappings based on common folder structures
        self.category_keywords = {
            'financials': ['financial', 'finance', 'budget', 'accounting', 'statements'],
            'meetings': ['meeting', 'notes', 'agenda', 'minutes', 'quarterly'],
            'projects': ['project', 'initiative', 'implementation'],
            'documents': ['handout', 'template', 'form', 'policy'],
            'behavioral': ['behavioral', 'profile', 'assessment', 'culture'],
            'training': ['training', 'onboarding', 'handbook'],
            'archived': ['archive', 'archived', 'old'],
            'airplane': ['airplane', 'travel', 'dashboard']
        }
        
    def extract_client_info(self, document_path: str) -> ClientInfo:
        """
        Extract client information from document path
        
        Args:
            document_path: Full path to document (e.g., "/ClientName (PM-X)/folder/file.pdf")
            
        Returns:
            ClientInfo object with extracted metadata
        """
        if not document_path:
            return self._create_unknown_client_info(document_path)
            
        # Check for client pattern
        client_match = self.client_pattern.search(document_path)
        
        if client_match:
            client_name = client_match.group(1).strip()
            pm_initial = client_match.group(2)
            
            # Extract folder path (everything after client folder)
            client_folder_end = client_match.end()
            remaining_path = document_path[client_folder_end:] if client_folder_end < len(document_path) else ""
            
            # Determine document category
            category = self._determine_document_category(document_path)
            
            return ClientInfo(
                client_name=client_name,
                pm_initial=pm_initial,
                pm_name=self.pm_names.get(pm_initial, "Unknown"),
                folder_path=remaining_path,
                is_client_specific=True,
                document_category=category
            )
        else:
            # Check if it's an internal/tool folder
            path_lower = document_path.lower()
            for internal_folder in self.internal_folders:
                if f'/{internal_folder}/' in path_lower or path_lower.startswith(f'/{internal_folder}/'):
                    category = self._determine_document_category(document_path)
                    return ClientInfo(
                        client_name="Autobahn Internal",
                        pm_initial="N/A",
                        pm_name="N/A",
                        folder_path=document_path,
                        is_client_specific=False,
                        document_category=category
                    )
            
            # Unknown/uncategorized
            return self._create_unknown_client_info(document_path)
    
    def _create_unknown_client_info(self, document_path: str) -> ClientInfo:
        """Create ClientInfo for unknown/uncategorized documents"""
        category = self._determine_document_category(document_path)
        return ClientInfo(
            client_name="Uncategorized",
            pm_initial="N/A",
            pm_name="N/A", 
            folder_path=document_path,
            is_client_specific=False,
            document_category=category
        )
    
    def _determine_document_category(self, document_path: str) -> str:
        """Determine document category based on path keywords"""
        path_lower = document_path.lower()
        
        # Check each category
        for category, keywords in self.category_keywords.items():
            if any(keyword in path_lower for keyword in keywords):
                return category
                
        # Default category
        return "general"
    
    def extract_metadata_batch(self, document_paths: List[str]) -> Dict[str, ClientInfo]:
        """
        Extract client metadata for a batch of document paths
        
        Args:
            document_paths: List of document paths
            
        Returns:
            Dictionary mapping document_path -> ClientInfo
        """
        results = {}
        for path in document_paths:
            try:
                results[path] = self.extract_client_info(path)
            except Exception as e:
                logging.error(f"Error processing path {path}: {str(e)}")
                results[path] = self._create_unknown_client_info(path)
        
        return results
    
    def get_client_statistics(self, client_infos: List[ClientInfo]) -> Dict[str, int]:
        """Get statistics about client distribution"""
        stats = {
            'total_documents': len(client_infos),
            'client_specific': sum(1 for info in client_infos if info.is_client_specific),
            'internal_documents': sum(1 for info in client_infos if not info.is_client_specific),
            'clients': {},
            'categories': {},
            'pms': {}
        }
        
        for info in client_infos:
            # Client counts
            if info.client_name in stats['clients']:
                stats['clients'][info.client_name] += 1
            else:
                stats['clients'][info.client_name] = 1
                
            # Category counts  
            if info.document_category in stats['categories']:
                stats['categories'][info.document_category] += 1
            else:
                stats['categories'][info.document_category] = 1
                
            # PM counts (only for client-specific docs)
            if info.is_client_specific and info.pm_initial != "N/A":
                if info.pm_initial in stats['pms']:
                    stats['pms'][info.pm_initial] += 1
                else:
                    stats['pms'][info.pm_initial] = 1
        
        return stats

# Test functions
def test_client_extraction():
    """Test the client extraction functionality"""
    extractor = ClientMetadataExtractor()
    
    test_paths = [
        "/Camelot (PM-C)/_08. Financials/Pages from May 2021 Financial Package.pdf",
        "/Phoenix Corporation (PM-S)/Handouts_ Phoenix Corp_ Amended (Dec 2024).pdf",
        "/LJ Kruse (PM-S)/LJ KRUSE - SHARED FOLDER (Do not move)/LJK  Onboarding  Goalsetting Template .xlsx",
        "/Autobahn Tools/1-3-1 Problem Solving/1-3-1 Handouts-JTL.pdf",
        "/PM & APM Training Materials/APM Handbook.pdf",
        "/Some Unknown Folder/random_document.pdf"
    ]
    
    print("🧪 Testing Client Metadata Extraction")
    print("=" * 50)
    
    for path in test_paths:
        info = extractor.extract_client_info(path)
        print(f"\nPath: {path}")
        print(f"  Client: {info.client_name}")
        print(f"  PM: {info.pm_initial} ({info.pm_name})")
        print(f"  Category: {info.document_category}")
        print(f"  Client-specific: {info.is_client_specific}")

if __name__ == "__main__":
    test_client_extraction()
