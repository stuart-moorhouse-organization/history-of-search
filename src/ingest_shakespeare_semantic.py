#!/usr/bin/env python3
"""
Add semantic search capabilities using the preconfigured ELSER endpoint.

This script creates a semantic index using the .elser-2-elasticsearch endpoint
that's already available in Elastic Cloud.
"""

import json
import os
from typing import Iterator, Dict, Any
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, reindex
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

def create_semantic_index_mapping(es: Elasticsearch) -> None:
    """
    Create the semantic Shakespeare index with semantic_text field.
    
    Args:
        es: Elasticsearch client
    """
    index_name = "shakespeare-semantic"
    
    # Use semantic_text field type which automatically handles ELSER embeddings
    mapping = {
        "mappings": {
            "properties": {
                "type": {"type": "keyword"},
                "line_id": {"type": "integer"},
                "play_name": {"type": "keyword"},
                "speech_number": {"type": "integer"},
                "line_number": {"type": "keyword"},
                "speaker": {"type": "keyword"},
                "text_entry": {
                    "type": "text"
                },
                "semantic_text": {
                    "type": "semantic_text",
                    "inference_id": ".elser-2-elasticsearch"
                }
            }
        }
    }
    
    # Delete index if it exists
    if es.indices.exists(index=index_name):
        print(f"Deleting existing {index_name} index...")
        es.indices.delete(index=index_name)
    
    # Create new index with mapping
    print(f"Creating {index_name} index with semantic_text mapping...")
    es.indices.create(index=index_name, body=mapping)

def reindex_with_semantic(es: Elasticsearch) -> None:
    """
    Reindex data from shakespeare to shakespeare-semantic with semantic text.
    
    Args:
        es: Elasticsearch client
    """
    print("Reindexing data with semantic text...")
    
    # Reindex and copy text_entry to semantic_text field
    reindex_body = {
        "source": {
            "index": "shakespeare"
        },
        "dest": {
            "index": "shakespeare-semantic"
        },
        "script": {
            "source": """
                ctx._source.semantic_text = ctx._source.text_entry;
            """
        }
    }
    
    # Start reindexing
    response = es.reindex(body=reindex_body, wait_for_completion=False)
    task_id = response['task']
    
    print(f"Reindexing started with task ID: {task_id}")
    print("This may take several minutes as ELSER processes each document...")
    
    # Monitor reindexing progress
    start_time = time.time()
    last_created = 0
    
    while True:
        task_status = es.tasks.get(task_id=task_id)
        
        if task_status['completed']:
            print("\nReindexing completed!")
            
            # Print results
            if 'response' in task_status:
                response = task_status['response']
                print(f"Documents processed: {response.get('total', 0)}")
                print(f"Documents created: {response.get('created', 0)}")
                
                if response.get('failures'):
                    print(f"Failures: {len(response['failures'])}")
                    for failure in response['failures'][:5]:  # Show first 5 failures
                        print(f"  - {failure}")
            break
        else:
            # Show progress if available
            if 'status' in task_status['task']:
                status = task_status['task']['status']
                total = status.get('total', 0)
                created = status.get('created', 0)
                if total > 0:
                    progress = (created / total) * 100
                    elapsed_time = int(time.time() - start_time)
                    
                    # Calculate rate
                    if created > last_created and elapsed_time > 0:
                        rate = (created - last_created) / 10  # docs per second (checking every 10s)
                        eta = (total - created) / rate if rate > 0 else 0
                        eta_min = int(eta / 60)
                        eta_sec = int(eta % 60)
                        print(f"\rProgress: {created}/{total} ({progress:.1f}%) - Rate: {rate:.1f} docs/s - ETA: {eta_min}m {eta_sec}s", end='', flush=True)
                    else:
                        print(f"\rProgress: {created}/{total} ({progress:.1f}%) - Elapsed: {elapsed_time}s", end='', flush=True)
                    
                    last_created = created
        
        time.sleep(10)  # Check every 10 seconds
    
    print()  # New line after progress
    
    # Refresh the semantic index
    es.indices.refresh(index="shakespeare-semantic")
    
    # Get final stats
    stats = es.indices.stats(index="shakespeare-semantic")
    doc_count = stats['indices']['shakespeare-semantic']['total']['docs']['count']
    print(f"Total documents in shakespeare-semantic index: {doc_count}")

def verify_semantic_search(es: Elasticsearch) -> None:
    """
    Test semantic search to verify it's working.
    
    Args:
        es: Elasticsearch client
    """
    print("\nTesting semantic search...")
    
    test_query = "mortality and death"
    
    # Semantic search query
    search_body = {
        "query": {
            "semantic": {
                "field": "semantic_text",
                "query": test_query
            }
        },
        "size": 3,
        "_source": ["play_name", "speaker", "text_entry"]
    }
    
    try:
        response = es.search(index="shakespeare-semantic", body=search_body)
        
        print(f"\nSemantic search results for '{test_query}':")
        for hit in response['hits']['hits']:
            source = hit['_source']
            print(f"\n- {source['play_name']} - {source.get('speaker', 'N/A')}")
            print(f"  {source['text_entry'][:200]}...")
            print(f"  Score: {hit['_score']:.4f}")
            
    except Exception as e:
        print(f"Error during semantic search test: {e}")

def main():
    """Main function to add semantic search capabilities to Shakespeare data."""
    
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
    
    # Check if the original shakespeare index exists
    if not es.indices.exists(index="shakespeare"):
        raise ValueError("Original shakespeare index not found. Please run ingest_shakespeare.py first.")
    
    # Check if .elser-2-elasticsearch endpoint exists
    print("Checking for .elser-2-elasticsearch endpoint...")
    try:
        # Try to get inference info
        inference_info = es.perform_request(
            method='GET',
            path='/_inference/.elser-2-elasticsearch'
        )
        print("Found .elser-2-elasticsearch endpoint!")
    except Exception as e:
        print(f"Warning: Could not verify .elser-2-elasticsearch endpoint: {e}")
        print("Continuing anyway as it should be preconfigured in Elastic Cloud...")
    
    try:
        # Step 1: Create semantic index with proper mapping
        create_semantic_index_mapping(es)
        
        # Step 2: Reindex data with semantic text
        reindex_with_semantic(es)
        
        # Step 3: Verify semantic search works
        verify_semantic_search(es)
        
        print("\n✅ Semantic Shakespeare index created successfully!")
        print("You can now use semantic search with the 'shakespeare-semantic' index.")
        print("\nExample query structure:")
        print('{')
        print('  "query": {')
        print('    "semantic": {')
        print('      "field": "semantic_text",')
        print('      "query": "your search query here"')
        print('    }')
        print('  }')
        print('}')
        
    except Exception as e:
        print(f"\nError during semantic index setup: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure you have an Elastic Cloud deployment with ML capabilities")
        print("2. Check that .elser-2-elasticsearch endpoint is available in Kibana")
        print("3. Verify you have sufficient ML node resources")
        raise

if __name__ == "__main__":
    main()