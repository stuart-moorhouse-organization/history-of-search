#!/usr/bin/env python3
"""
Ingest Shakespeare dataset into Elasticsearch.

This script loads the Shakespeare dataset and ingests it into Elasticsearch
using the bulk API for efficient indexing.
"""

import json
import os
from typing import Iterator, Dict, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_shakespeare_data(file_path: str) -> Iterator[Dict[str, Any]]:
    """
    Load Shakespeare data from JSONL file.
    
    Args:
        file_path: Path to the shakespeare.json file
        
    Yields:
        Document dictionaries ready for Elasticsearch indexing
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    # Process pairs of lines (index instruction + document)
    for i in range(0, len(lines), 2):
        if i + 1 < len(lines):
            index_line = json.loads(lines[i])
            doc_line = json.loads(lines[i + 1])
            
            # Extract index info
            index_info = index_line['index']
            doc_id = index_info['_id']
            
            # Prepare document for bulk API
            doc = {
                '_index': 'shakespeare',
                '_id': doc_id,
                '_source': doc_line
            }
            
            yield doc

def create_index_mapping(es: Elasticsearch) -> None:
    """
    Create the Shakespeare index with proper mapping.
    
    Args:
        es: Elasticsearch client
    """
    mapping = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "english_no_stop": {
                        "type": "english",
                        "stopwords": "_none_"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "type": {"type": "keyword"},
                "line_id": {"type": "integer"},
                "play_name": {"type": "keyword"},
                "speech_number": {"type": "integer"},
                "line_number": {"type": "keyword"},
                "speaker": {"type": "keyword"},
                "text_entry": {
                    "type": "text",
                    "analyzer": "english_no_stop"
                }
            }
        }
    }
    
    # Delete index if it exists
    if es.indices.exists(index="shakespeare"):
        print("Deleting existing shakespeare index...")
        es.indices.delete(index="shakespeare")
    
    # Create new index with mapping
    print("Creating shakespeare index with mapping (no stop words)...")
    es.indices.create(index="shakespeare", body=mapping)

def ingest_data(es: Elasticsearch, file_path: str) -> None:
    """
    Ingest Shakespeare data into Elasticsearch.
    
    Args:
        es: Elasticsearch client
        file_path: Path to the shakespeare.json file
    """
    print(f"Loading data from {file_path}...")
    
    # Create index with mapping
    create_index_mapping(es)
    
    # Bulk index documents
    print("Ingesting documents...")
    documents = load_shakespeare_data(file_path)
    
    # Use bulk helper for efficient indexing
    success_count, failed_count = bulk(
        es,
        documents,
        chunk_size=1000,
        request_timeout=60
    )
    
    print(f"Successfully indexed: {success_count} documents")
    if failed_count:
        print(f"Failed to index: {len(failed_count)} documents")
    
    # Refresh index to make documents available for search
    es.indices.refresh(index="shakespeare")
    
    # Get index stats
    stats = es.indices.stats(index="shakespeare")
    doc_count = stats['indices']['shakespeare']['total']['docs']['count']
    print(f"Total documents in shakespeare index: {doc_count}")

def main():
    """Main function to run the ingestion."""
    
    # Get Elasticsearch connection details from environment
    cloud_id = os.getenv('ELASTIC_CLOUD_ID')
    api_key = os.getenv('ELASTIC_API_KEY')
    
    if not cloud_id or not api_key:
        raise ValueError("Please set ELASTIC_CLOUD_ID and ELASTIC_API_KEY in your .env file")
    
    # Initialize Elasticsearch client
    print("Connecting to Elasticsearch...")
    es = Elasticsearch(
        cloud_id=cloud_id,
        api_key=api_key
    )
    
    # Test connection
    if not es.ping():
        raise ConnectionError("Could not connect to Elasticsearch")
    
    print("Connected to Elasticsearch successfully!")
    
    # Path to shakespeare data file
    shakespeare_file = "shakespeare.json"
    
    if not os.path.exists(shakespeare_file):
        raise FileNotFoundError(f"Shakespeare data file not found: {shakespeare_file}")
    
    # Ingest the data
    ingest_data(es, shakespeare_file)
    
    print("Shakespeare data ingestion completed!")

if __name__ == "__main__":
    main()