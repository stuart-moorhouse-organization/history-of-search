#!/usr/bin/env python3
"""
Add ELSER semantic embeddings to Shakespeare dataset.

This script creates a new index with ELSER semantic embeddings for semantic search,
using the inference API and ingest pipeline approach.
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

def deploy_elser_model(es: Elasticsearch) -> None:
    """
    Deploy ELSER v2 model if not already deployed.
    
    Args:
        es: Elasticsearch client
    """
    print("Checking if ELSER model is deployed...")
    
    try:
        # Check if ELSER model is already deployed
        models = es.ml.get_trained_models()
        elser_deployed = any(
            model['model_id'] == '.elser_model_2' 
            for model in models.get('trained_models', [])
        )
        
        if elser_deployed:
            print("ELSER v2 model already deployed!")
            return
            
    except Exception as e:
        print(f"Error checking models: {e}")
    
    print("Deploying ELSER v2 model...")
    
    # Deploy ELSER v2 model
    try:
        es.ml.start_trained_model_deployment(
            model_id=".elser_model_2",
            body={
                "number_of_allocations": 1,
                "threads_per_allocation": 1,
                "priority": "normal"
            }
        )
        
        print("ELSER model deployment started. Waiting for deployment to complete...")
        
        # Wait for deployment to complete
        max_wait_time = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                deployment_stats = es.ml.get_trained_model_deployment_stats(
                    model_id=".elser_model_2"
                )
                
                if deployment_stats['deployment_stats']:
                    state = deployment_stats['deployment_stats'][0]['state']
                    if state == 'started':
                        print("ELSER model deployed successfully!")
                        return
                    elif state == 'starting':
                        print("Model is starting...")
                        time.sleep(10)
                    else:
                        print(f"Model state: {state}")
                        time.sleep(10)
                else:
                    time.sleep(10)
                    
            except Exception as e:
                print(f"Waiting for deployment... ({e})")
                time.sleep(10)
        
        raise TimeoutError("ELSER model deployment timed out")
        
    except Exception as e:
        print(f"Error deploying ELSER model: {e}")
        print("Note: You may need to upgrade your Elasticsearch subscription for ELSER access")
        raise

def create_elser_inference_endpoint(es: Elasticsearch) -> None:
    """
    Create ELSER inference endpoint.
    
    Args:
        es: Elasticsearch client
    """
    print("Creating ELSER inference endpoint...")
    
    try:
        # Delete existing endpoint if it exists
        try:
            es.options(ignore_status=404).delete("_inference/elser-endpoint")
        except:
            pass  # Endpoint doesn't exist, that's fine
        
        # Create inference endpoint
        es.put(
            "_inference/elser-endpoint",
            body={
                "service": "elser",
                "service_settings": {
                    "model_id": ".elser_model_2"
                }
            }
        )
        
        print("ELSER inference endpoint created successfully!")
        
    except Exception as e:
        print(f"Error creating inference endpoint: {e}")
        raise

def create_ingest_pipeline(es: Elasticsearch) -> None:
    """
    Create ingest pipeline for ELSER embeddings.
    
    Args:
        es: Elasticsearch client
    """
    print("Creating ELSER ingest pipeline...")
    
    pipeline = {
        "processors": [
            {
                "inference": {
                    "model_id": ".elser_model_2",
                    "input_output": [
                        {
                            "input_field": "text_entry",
                            "output_field": "text_entry_embedding"
                        }
                    ]
                }
            }
        ]
    }
    
    # Delete existing pipeline if it exists
    try:
        es.ingest.delete_pipeline(id="elser-pipeline")
    except:
        pass  # Pipeline doesn't exist, that's fine
    
    # Create new pipeline
    es.ingest.put_pipeline(id="elser-pipeline", body=pipeline)
    print("ELSER ingest pipeline created successfully!")

def create_semantic_index_mapping(es: Elasticsearch) -> None:
    """
    Create the semantic Shakespeare index with ELSER mapping.
    
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
                "text_entry_embedding": {
                    "type": "sparse_vector"
                }
            }
        }
    }
    
    # Delete index if it exists
    if es.indices.exists(index=index_name):
        print(f"Deleting existing {index_name} index...")
        es.indices.delete(index=index_name)
    
    # Create new index with mapping
    print(f"Creating {index_name} index with ELSER mapping...")
    es.indices.create(index=index_name, body=mapping)

def reindex_with_elser(es: Elasticsearch) -> None:
    """
    Reindex data from shakespeare to shakespeare-semantic with ELSER embeddings.
    
    Args:
        es: Elasticsearch client
    """
    print("Reindexing data with ELSER embeddings...")
    
    # Reindex with the ingest pipeline
    reindex_body = {
        "source": {
            "index": "shakespeare"
        },
        "dest": {
            "index": "shakespeare-semantic",
            "pipeline": "elser-pipeline"
        }
    }
    
    # Start reindexing
    response = es.reindex(body=reindex_body, wait_for_completion=False)
    task_id = response['task']
    
    print(f"Reindexing started with task ID: {task_id}")
    print("This may take several minutes as ELSER processes each document...")
    
    # Monitor reindexing progress
    while True:
        task_status = es.tasks.get(task_id=task_id)
        
        if task_status['completed']:
            print("Reindexing completed!")
            
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
                    print(f"Progress: {created}/{total} ({progress:.1f}%)")
        
        time.sleep(10)  # Check every 10 seconds
    
    # Refresh the semantic index
    es.indices.refresh(index="shakespeare-semantic")
    
    # Get final stats
    stats = es.indices.stats(index="shakespeare-semantic")
    doc_count = stats['indices']['shakespeare-semantic']['total']['docs']['count']
    print(f"Total documents in shakespeare-semantic index: {doc_count}")

def main():
    """Main function to add ELSER embeddings to Shakespeare data."""
    
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
    
    try:
        # Step 1: Deploy ELSER model
        deploy_elser_model(es)
        
        # Step 2: Create inference endpoint
        create_elser_inference_endpoint(es)
        
        # Step 3: Create ingest pipeline
        create_ingest_pipeline(es)
        
        # Step 4: Create semantic index with proper mapping
        create_semantic_index_mapping(es)
        
        # Step 5: Reindex data with ELSER embeddings
        reindex_with_elser(es)
        
        print("\nSemantic Shakespeare index created successfully!")
        print("You can now use semantic search with the 'shakespeare-semantic' index.")
        
    except Exception as e:
        print(f"Error during ELSER setup: {e}")
        print("\nNote: ELSER requires an appropriate Elasticsearch subscription.")
        print("If you're using Elastic Cloud, make sure you have:")
        print("- A trial or paid subscription that includes ML features")
        print("- Sufficient ML node resources (minimum 4GB)")
        raise

if __name__ == "__main__":
    main()