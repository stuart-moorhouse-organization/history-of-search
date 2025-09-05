#!/usr/bin/env python3
"""
Add both sparse (ELSER) and dense (E5) semantic embeddings to Shakespeare dataset.

This script creates a new index with:
- ELSER sparse vector embeddings (text_entry_embedding field)
- E5 multilingual dense embeddings (text_entry_dense field)
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

def create_dual_semantic_index_mapping(es: Elasticsearch) -> None:
    """
    Create Shakespeare index with both sparse and dense semantic fields.
    
    Args:
        es: Elasticsearch client
    """
    index_name = "shakespeare-semantic"
    
    mapping = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "phrase_search": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "asciifolding"
                        ]
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
                    "analyzer": "phrase_search"
                },
                # ELSER sparse vector field
                "text_entry_embedding": {
                    "type": "sparse_vector"
                },
                # E5 dense vector field (semantic_text)
                "text_entry_dense": {
                    "type": "semantic_text",
                    "inference_id": ".multilingual-e5-small-elasticsearch"
                }
            }
        }
    }
    
    # Delete index if it exists
    if es.indices.exists(index=index_name):
        print(f"Deleting existing {index_name} index...")
        es.indices.delete(index=index_name)
    
    # Create new index with mapping
    print(f"Creating {index_name} index with dual semantic mappings...")
    print("- text_entry_embedding: sparse_vector (for ELSER)")
    print("- text_entry_dense: semantic_text (for E5 multilingual)")
    es.indices.create(index=index_name, body=mapping)

def create_dual_ingest_pipeline(es: Elasticsearch) -> None:
    """
    Create ingest pipeline for both ELSER and E5 embeddings.
    
    Args:
        es: Elasticsearch client
    """
    print("Creating dual semantic ingest pipeline...")
    
    pipeline = {
        "processors": [
            # ELSER processor for sparse vectors
            {
                "inference": {
                    "model_id": ".elser_model_2",
                    "input_output": [
                        {
                            "input_field": "text_entry",
                            "output_field": "text_entry_embedding"
                        }
                    ],
                    "on_failure": [
                        {
                            "set": {
                                "field": "_ingest.on_failure_message",
                                "value": "ELSER processing failed: {{_ingest.on_failure_message}}"
                            }
                        }
                    ]
                }
            }
        ]
    }
    
    # Delete existing pipeline if it exists
    try:
        es.ingest.delete_pipeline(id="dual-semantic-pipeline")
    except:
        pass  # Pipeline doesn't exist, that's fine
    
    # Create new pipeline
    es.ingest.put_pipeline(id="dual-semantic-pipeline", body=pipeline)
    print("Dual semantic ingest pipeline created successfully!")
    print("Note: E5 embeddings will be handled automatically by semantic_text field")

def reindex_with_dual_semantics(es: Elasticsearch) -> None:
    """
    Reindex data from shakespeare to shakespeare-semantic with both embeddings.
    
    Args:
        es: Elasticsearch client
    """
    print("Reindexing data with dual semantic embeddings...")
    print("This will generate both ELSER sparse vectors and E5 dense embeddings...")
    
    # Reindex with the ingest pipeline and copy text_entry to text_entry_dense
    reindex_body = {
        "source": {
            "index": "shakespeare"
        },
        "dest": {
            "index": "shakespeare-semantic",
            "pipeline": "dual-semantic-pipeline"
        },
        "script": {
            "source": """
                // Copy text_entry to text_entry_dense for E5 processing
                ctx._source.text_entry_dense = ctx._source.text_entry;
            """
        }
    }
    
    # Start reindexing
    response = es.reindex(body=reindex_body, wait_for_completion=False)
    task_id = response['task']
    
    print(f"Reindexing started with task ID: {task_id}")
    print("This may take 30-40 minutes as both ELSER and E5 process each document...")
    
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

def verify_dual_semantic_search(es: Elasticsearch) -> None:
    """
    Test both sparse and dense semantic search to verify they're working.
    
    Args:
        es: Elasticsearch client
    """
    print("\nTesting dual semantic search capabilities...")
    
    test_query = "mortality and death"
    
    # Test sparse vector (ELSER) search
    print(f"\n1. Testing sparse vector (ELSER) search for '{test_query}':")
    sparse_search_body = {
        "query": {
            "sparse_vector": {
                "field": "text_entry_embedding",
                "inference_id": ".elser-2-elasticsearch",
                "query": test_query
            }
        },
        "size": 2,
        "_source": ["play_name", "speaker", "text_entry"]
    }
    
    try:
        response = es.search(index="shakespeare-semantic", body=sparse_search_body)
        for hit in response['hits']['hits']:
            source = hit['_source']
            print(f"  - {source['play_name']} - {source.get('speaker', 'N/A')}")
            print(f"    {source['text_entry'][:100]}...")
            print(f"    Score: {hit['_score']:.4f}")
    except Exception as e:
        print(f"  Error during sparse vector search: {e}")
    
    # Test dense vector (E5) search
    print(f"\n2. Testing dense vector (E5) search for '{test_query}':")
    dense_search_body = {
        "query": {
            "semantic": {
                "field": "text_entry_dense",
                "query": test_query
            }
        },
        "size": 2,
        "_source": ["play_name", "speaker", "text_entry"]
    }
    
    try:
        response = es.search(index="shakespeare-semantic", body=dense_search_body)
        for hit in response['hits']['hits']:
            source = hit['_source']
            print(f"  - {source['play_name']} - {source.get('speaker', 'N/A')}")
            print(f"    {source['text_entry'][:100]}...")
            print(f"    Score: {hit['_score']:.4f}")
    except Exception as e:
        print(f"  Error during dense vector search: {e}")

def main():
    """Main function to add dual semantic embeddings to Shakespeare data."""
    
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
    
    # Check if ELSER is available
    print("\nChecking for .elser-2-elasticsearch endpoint...")
    try:
        inference_info = es.perform_request(
            method='GET',
            path='/_inference/.elser-2-elasticsearch'
        )
        print("Found .elser-2-elasticsearch endpoint!")
    except Exception as e:
        print(f"Warning: Could not verify .elser-2-elasticsearch endpoint: {e}")
        print("Continuing anyway as it should be preconfigured in Elastic Cloud...")
    
    # Check if E5 is available
    print("\nChecking for .multilingual-e5-small-elasticsearch endpoint...")
    try:
        inference_info = es.perform_request(
            method='GET',
            path='/_inference/.multilingual-e5-small-elasticsearch'
        )
        print("Found .multilingual-e5-small-elasticsearch endpoint!")
    except Exception as e:
        print(f"Warning: Could not verify .multilingual-e5-small-elasticsearch endpoint: {e}")
        print("Continuing anyway as it should be preconfigured in Elastic Cloud...")
    
    try:
        # Step 1: Create dual semantic index with proper mappings
        create_dual_semantic_index_mapping(es)
        
        # Step 2: Create ingest pipeline for ELSER
        create_dual_ingest_pipeline(es)
        
        # Step 3: Reindex data with both embeddings
        reindex_with_dual_semantics(es)
        
        # Step 4: Verify both search types work
        verify_dual_semantic_search(es)
        
        print("\n✅ Dual semantic Shakespeare index created successfully!")
        print("You can now use:")
        print("1. Sparse vector search with text_entry_embedding field (ELSER)")
        print("2. Dense vector search with text_entry_dense field (E5 multilingual)")
        
    except Exception as e:
        print(f"\nError during dual semantic index setup: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure you have an Elastic Cloud deployment with ML capabilities")
        print("2. Check that both .elser-2-elasticsearch and .multilingual-e5-small-elasticsearch endpoints are available")
        print("3. Verify you have sufficient ML node resources")
        raise

if __name__ == "__main__":
    main()