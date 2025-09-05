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
                "bool": {
                    "should": [
                        # Exact phrase match gets highest score
                        {
                            "match_phrase": {
                                "text_entry": {
                                    "query": query,
                                    "boost": 10
                                }
                            }
                        },
                        # Phrase with some flexibility 
                        {
                            "match_phrase": {
                                "text_entry": {
                                    "query": query,
                                    "slop": 3,
                                    "boost": 5
                                }
                            }
                        },
                        # Multi-match for partial matches
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["text_entry"],
                                "type": "phrase",
                                "slop": 2,
                                "boost": 2
                            }
                        },
                        # Individual terms (fallback with partial matching)
                        {
                            "match": {
                                "text_entry": {
                                    "query": query,
                                    "operator": "or",
                                    "minimum_should_match": "60%",
                                    "boost": 1
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
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
            },
            "elasticsearch_query": search_body  # Include the actual query used
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
    
    def get_document_by_line_id(self, line_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific document by line ID.
        
        Args:
            line_id: The line ID to retrieve
            
        Returns:
            Document data or None if not found
        """
        try:
            response = self.es.search(
                index="shakespeare",
                body={
                    "query": {
                        "term": {
                            "line_id": line_id
                        }
                    },
                    "size": 1
                }
            )
            
            if response["hits"]["total"]["value"] > 0:
                hit = response["hits"]["hits"][0]
                return hit["_source"]
            return None
        except Exception:
            return None
    
    def search_shakespeare_semantic_sparse(
        self, 
        query: str, 
        selected_plays: Optional[List[str]] = None,
        from_: int = 0,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        Search Shakespeare texts using sparse vector semantic search with ELSER.
        
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
        
        # Add sparse vector search if query provided
        if query:
            must_clauses.append({
                "sparse_vector": {
                    "field": "text_entry_embedding",
                    "inference_id": ".elser-2-elasticsearch",
                    "query": query
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
        
        # Execute search on semantic index
        response = self.es.search(
            index="shakespeare-semantic",
            body=search_body
        )
        
        # Process results
        results = {
            "total": response["hits"]["total"]["value"],
            "hits": [],
            "aggregations": {
                "plays": []
            },
            "elasticsearch_query": search_body  # Include the actual query used
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
    
    def search_shakespeare_semantic_dense(
        self, 
        query: str, 
        selected_plays: Optional[List[str]] = None,
        from_: int = 0,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        Search Shakespeare texts using dense vector semantic search with E5.
        
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
        
        # Add semantic (dense vector) search if query provided
        if query:
            must_clauses.append({
                "semantic": {
                    "field": "text_entry_dense",
                    "query": query
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
        
        # Execute search on semantic index
        response = self.es.search(
            index="shakespeare-semantic",
            body=search_body
        )
        
        # Process results
        results = {
            "total": response["hits"]["total"]["value"],
            "hits": [],
            "aggregations": {
                "plays": []
            },
            "elasticsearch_query": search_body  # Include the actual query used
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
    
    def search_shakespeare_hybrid(
        self, 
        query: str, 
        selected_plays: Optional[List[str]] = None,
        from_: int = 0,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        Hybrid search using RRF to combine term-based and semantic search.
        
        Args:
            query: Search query text
            selected_plays: List of play names to filter by
            from_: Starting position for pagination
            size: Number of results to return
            
        Returns:
            Search results with hits and aggregations
        """
        # Build the RRF retriever query
        retrievers = []
        
        if query:
            # Add standard retriever for term-based search
            standard_query = {
                "bool": {
                    "should": [
                        # Exact phrase match gets highest score
                        {
                            "match_phrase": {
                                "text_entry": {
                                    "query": query,
                                    "boost": 10
                                }
                            }
                        },
                        # Phrase with some flexibility
                        {
                            "match_phrase": {
                                "text_entry": {
                                    "query": query,
                                    "slop": 3,
                                    "boost": 5
                                }
                            }
                        },
                        # Individual terms
                        {
                            "match": {
                                "text_entry": {
                                    "query": query,
                                    "operator": "or",
                                    "minimum_should_match": "60%",
                                    "boost": 1
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
            
            # Apply play filter to standard retriever if needed
            if selected_plays:
                standard_query = {
                    "bool": {
                        "must": [standard_query],
                        "filter": [{"terms": {"play_name": selected_plays}}]
                    }
                }
            
            retrievers.append({
                "standard": {
                    "query": standard_query
                }
            })
            
            # Add semantic retriever if semantic index exists
            try:
                # Check if semantic index exists
                if self.es.indices.exists(index="shakespeare-semantic"):
                    semantic_query = {
                        "semantic": {
                            "field": "semantic_text",
                            "query": query
                        }
                    }
                    
                    # Apply play filter to semantic query if needed
                    if selected_plays:
                        semantic_query = {
                            "bool": {
                                "must": [semantic_query],
                                "filter": [{"terms": {"play_name": selected_plays}}]
                            }
                        }
                    
                    # For semantic retriever, we need to specify the index
                    # Since RRF needs to work across indices, we'll use a different approach
                    # We'll execute both searches and combine manually
                    pass  # Will implement manual RRF below
            except:
                pass
        
        # Since Elasticsearch Python client doesn't directly support retrievers API yet,
        # we'll implement RRF manually by executing both searches and combining results
        
        # Execute term-based search
        term_results = self.search_shakespeare(query, selected_plays, 0, 100)
        
        # Execute dense vector semantic search if index exists
        semantic_results = None
        try:
            if self.es.indices.exists(index="shakespeare-semantic"):
                semantic_results = self.search_shakespeare_semantic_dense(query, selected_plays, 0, 100)
        except:
            pass
        
        # Implement RRF manually
        rrf_scores = {}
        k = 60  # RRF constant
        
        # Score term-based results
        for i, hit in enumerate(term_results["hits"]):
            line_id = hit["line_id"]
            rrf_scores[line_id] = rrf_scores.get(line_id, 0) + 1.0 / (i + 1 + k)
            
        # Score semantic results if available
        if semantic_results:
            for i, hit in enumerate(semantic_results["hits"]):
                line_id = hit["line_id"]
                rrf_scores[line_id] = rrf_scores.get(line_id, 0) + 1.0 / (i + 1 + k)
        
        # Sort by RRF score
        sorted_line_ids = sorted(rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True)
        
        # Build result set
        hits_by_id = {}
        for hit in term_results["hits"]:
            hits_by_id[hit["line_id"]] = hit
        if semantic_results:
            for hit in semantic_results["hits"]:
                if hit["line_id"] not in hits_by_id:
                    hits_by_id[hit["line_id"]] = hit
        
        # Get paginated results
        paginated_ids = sorted_line_ids[from_:from_ + size]
        paginated_hits = [hits_by_id[line_id] for line_id in paginated_ids if line_id in hits_by_id]
        
        # Build the response
        results = {
            "total": len(sorted_line_ids),
            "hits": paginated_hits,
            "aggregations": term_results["aggregations"],  # Use aggregations from term search
            "elasticsearch_query": {
                "description": "Hybrid search using Reciprocal Rank Fusion (RRF)",
                "rrf": {
                    "retrievers": [
                        {"type": "standard", "index": "shakespeare"},
                        {"type": "semantic_dense", "index": "shakespeare-semantic"} if semantic_results else None
                    ],
                    "rank_constant": k,
                    "rank_window_size": 100
                },
                "note": "This is a manual implementation of RRF combining term and dense semantic search"
            }
        }
        
        # Filter out None retriever if semantic search wasn't used
        results["elasticsearch_query"]["rrf"]["retrievers"] = [
            r for r in results["elasticsearch_query"]["rrf"]["retrievers"] if r
        ]
        
        return results
    
    def get_document_context(self, play_name: str, line_id: int, context_size: int = 50) -> List[Dict[str, Any]]:
        """
        Get surrounding context for a document (previous and next lines in the same play).
        
        Args:
            play_name: Name of the play
            line_id: Center line ID
            context_size: Number of lines before and after to include
            
        Returns:
            List of documents in order
        """
        try:
            response = self.es.search(
                index="shakespeare",
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"play_name": play_name}},
                                {"range": {"line_id": {"gte": max(1, line_id - context_size), "lte": line_id + context_size}}}
                            ]
                        }
                    },
                    "sort": [{"line_id": {"order": "asc"}}],
                    "size": context_size * 2 + 1,
                    "_source": ["play_name", "speaker", "text_entry", "line_id", "type"]
                }
            )
            
            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                source["is_current"] = source["line_id"] == line_id
                results.append(source)
            
            return results
        except Exception:
            return []