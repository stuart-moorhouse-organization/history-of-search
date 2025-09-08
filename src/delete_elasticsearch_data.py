#!/usr/bin/env python3
"""
Delete existing Shakespeare data from Elasticsearch.
"""

import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def delete_elasticsearch_indices():
    """Delete all Shakespeare-related indices from Elasticsearch."""
    
    # Initialize Elasticsearch connection
    cloud_id = os.getenv('ELASTIC_CLOUD_ID')
    api_key = os.getenv('ELASTIC_API_KEY')
    
    if not cloud_id or not api_key:
        raise ValueError("Please set ELASTIC_CLOUD_ID and ELASTIC_API_KEY in your .env file")
    
    es = Elasticsearch(
        cloud_id=cloud_id,
        api_key=api_key
    )
    
    if not es.ping():
        raise ConnectionError("Could not connect to Elasticsearch")
    
    indices_to_delete = ['shakespeare', 'shakespeare-semantic']
    
    for index in indices_to_delete:
        try:
            if es.indices.exists(index=index):
                print(f"Deleting index: {index}")
                es.indices.delete(index=index)
                print(f"Successfully deleted index: {index}")
            else:
                print(f"Index {index} does not exist, skipping...")
        except Exception as e:
            print(f"Error deleting index {index}: {e}")
    
    print("\nIndex deletion complete!")

if __name__ == "__main__":
    print("Deleting Elasticsearch indices...")
    delete_elasticsearch_indices()
    print("Done!")