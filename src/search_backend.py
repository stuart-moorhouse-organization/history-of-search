"""
Elasticsearch backend for search functionality.
"""

import os
from typing import Dict, Any, List, Optional
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SearchBackend:
    def __init__(self):
        """Initialize Elasticsearch connection."""
        cloud_id = os.getenv('ELASTIC_CLOUD_ID')
        api_key = os.getenv('ELASTIC_API_KEY')
        
        if not cloud_id or not api_key:
            raise ValueError("Please set ELASTIC_CLOUD_ID and ELASTIC_API_KEY in your .env file")
        
        self.es = Elasticsearch(
            cloud_id=cloud_id,
            api_key=api_key
        )
        
        if not self.es.ping():
            raise ConnectionError("Could not connect to Elasticsearch")
    
    def search_shakespeare(
        self, 
        query: str, 
        selected_plays: Optional[List[str]] = None,
        from_: int = 0,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        Search Shakespeare texts with optional play name filtering.
        
        Args:
            query: Search query text
            selected_plays: List of play names to filter by
            from_: Starting position for pagination
            size: Number of results to return
            
        Returns:
            Search results with hits and aggregations
        """
        # Build the query
        must_clauses = []
        
        # Add text search if query provided
        if query:
            must_clauses.append({
                "match": {
                    "text_entry": {
                        "query": query,
                        "operator": "or"
                    }
                }
            })
        
        # Add play name filter if plays selected
        if selected_plays:
            must_clauses.append({
                "terms": {
                    "play_name": selected_plays
                }
            })
        
        # Construct the search body
        search_body = {
            "query": {
                "bool": {
                    "must": must_clauses
                }
            } if must_clauses else {
                "match_all": {}
            },
            "aggs": {
                "plays": {
                    "terms": {
                        "field": "play_name",
                        "size": 50,  # Get all plays
                        "order": {"_key": "asc"}
                    }
                }
            },
            "from": from_,
            "size": size,
            "highlight": {
                "fields": {
                    "text_entry": {
                        "fragment_size": 200,
                        "number_of_fragments": 1
                    }
                },
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"]
            },
            "_source": ["play_name", "speaker", "text_entry", "line_id", "type"]
        }
        
        # Execute search
        response = self.es.search(
            index="shakespeare",
            body=search_body
        )
        
        # Process results
        results = {
            "total": response["hits"]["total"]["value"],
            "hits": [],
            "aggregations": {
                "plays": []
            }
        }
        
        # Process hits
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            result = {
                "play_name": source.get("play_name", ""),
                "speaker": source.get("speaker", ""),
                "text_entry": source.get("text_entry", ""),
                "line_id": source.get("line_id", 0),
                "type": source.get("type", ""),
                "highlight": hit.get("highlight", {}).get("text_entry", [source.get("text_entry", "")])
            }
            results["hits"].append(result)
        
        # Process aggregations
        for bucket in response["aggregations"]["plays"]["buckets"]:
            results["aggregations"]["plays"].append({
                "name": bucket["key"],
                "count": bucket["doc_count"]
            })
        
        return results