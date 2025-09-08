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
    
    def _get_min_word_count_filter(self):
        """
        Get a filter that excludes documents with 3 or fewer words.
        Uses a regex pattern to match text with at least 4 words.
        """
        return {
            "regexp": {
                "text_entry": {
                    "value": ".*\\s+.*\\s+.*\\s+.*",
                    "case_insensitive": True
                }
            }
        }
    
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
        
        # Build filter clauses
        filter_clauses = []
        
        # Add play name filter if plays selected
        if selected_plays:
            filter_clauses.append({
                "terms": {
                    "play_name": selected_plays
                }
            })
        
        # Add minimum word count filter - disabled
        # filter_clauses.append(self._get_min_word_count_filter())
        
        # Construct the search body
        search_body = {
            "query": {
                "bool": {
                    "must": must_clauses if must_clauses else [{"match_all": {}}],
                    "filter": filter_clauses
                }
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
        
        # Build filter clauses
        filter_clauses = []
        
        # Add play name filter if plays selected
        if selected_plays:
            filter_clauses.append({
                "terms": {
                    "play_name": selected_plays
                }
            })
        
        # Add minimum word count filter - disabled
        # filter_clauses.append(self._get_min_word_count_filter())
        
        # Construct the search body
        search_body = {
            "query": {
                "bool": {
                    "must": must_clauses if must_clauses else [{"match_all": {}}],
                    "filter": filter_clauses
                }
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
            "_source": ["play_name", "speaker", "text_entry", "line_id", "type", "text_entry_embedding"]
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
            "elasticsearch_query": search_body,  # Include the actual query used
            "first_hit_embedding": None  # Will store the embedding from first result
        }
        
        # Process hits
        for i, hit in enumerate(response["hits"]["hits"]):
            source = hit["_source"]
            result = {
                "play_name": source.get("play_name", ""),
                "speaker": source.get("speaker", ""),
                "text_entry": source.get("text_entry", ""),
                "line_id": source.get("line_id", 0),
                "type": source.get("type", ""),
                "highlight": hit.get("highlight", {}).get("text_entry", [source.get("text_entry", "")])
            }
            
            # Store the embedding from the first hit for display
            if i == 0 and "text_entry_embedding" in source:
                results["first_hit_embedding"] = source["text_entry_embedding"]
            
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
        size: int = 20,
        k: int = 10,
        similarity: float = 0.7
    ) -> Dict[str, Any]:
        """
        Search Shakespeare texts using pure KNN dense vector search with E5.
        
        Args:
            query: Search query text
            selected_plays: List of play names to filter by
            from_: Starting position for pagination
            size: Number of results to return
            k: Number of nearest neighbors to retrieve (default: 10)
            similarity: Minimum cosine similarity score (default: 0.7)
            
        Returns:
            Search results with hits and aggregations
        """
        # Build the KNN query
        knn_clause = None
        filter_clauses = []
        
        # Add KNN search if query provided
        if query:
            # Use pure KNN search with dense_vector field
            knn_clause = {
                "field": "text_entry_vector",  # Using the dense_vector field
                "query_vector_builder": {
                    "text_embedding": {
                        "model_id": ".multilingual-e5-small-elasticsearch",
                        "model_text": query
                    }
                },
                "k": k,
                "num_candidates": min(k * 10, 100),  # More candidates for better results
                "similarity": similarity  # Minimum cosine similarity threshold
            }
            
            # Add filters for selected plays and word count
            filters = []
            if selected_plays:
                filters.append({
                    "terms": {
                        "play_name": selected_plays
                    }
                })
            # filters.append(self._get_min_word_count_filter())
            
            if filters:
                knn_clause["filter"] = {
                    "bool": {
                        "must": filters
                    }
                }
        
        # If no query, fall back to match_all with filters
        if not knn_clause:
            # No KNN search, just return filtered results
            if selected_plays:
                filter_clauses.append({
                    "terms": {
                        "play_name": selected_plays
                    }
                })
            # Add word count filter - disabled
            # filter_clauses.append(self._get_min_word_count_filter())
            
            search_body = {
                "query": {
                    "bool": {
                        "filter": filter_clauses
                    }
                } if filter_clauses else {"match_all": {}},
                "aggs": {
                    "plays": {
                        "terms": {
                            "field": "play_name",
                            "size": 50,
                            "order": {"_key": "asc"}
                        }
                    }
                },
                "from": from_,
                "size": size,
                "_source": ["play_name", "speaker", "text_entry", "line_id", "type"]
            }
        else:
            # Use KNN search
            search_body = {
                "knn": knn_clause,
                "aggs": {
                    "plays": {
                        "terms": {
                            "field": "play_name",
                            "size": 50,
                            "order": {"_key": "asc"}
                        }
                    }
                },
                "from": from_,
                "size": size,
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
        Hybrid search using native Elasticsearch RRF to combine term-based and semantic search.
        
        Args:
            query: Search query text
            selected_plays: List of play names to filter by
            from_: Starting position for pagination
            size: Number of results to return
            
        Returns:
            Search results with hits and aggregations
        """
        if not query:
            # If no query, just return all results with optional filter
            filters = []
            if selected_plays:
                filters.append({"terms": {"play_name": selected_plays}})
            # filters.append(self._get_min_word_count_filter())
            
            filter_query = {
                "bool": {
                    "must": filters
                }
            } if filters else {"match_all": {}}
            
            search_body = {
                "query": filter_query,
                "aggs": {
                    "plays": {
                        "terms": {
                            "field": "play_name",
                            "size": 50,
                            "order": {"_key": "asc"}
                        }
                    }
                },
                "from": from_,
                "size": size,
                "_source": ["play_name", "speaker", "text_entry", "line_id", "type"]
            }
            
            response = self.es.search(
                index="shakespeare-semantic",
                body=search_body
            )
        else:
            # Build RRF query with retrievers
            retrievers = []
            
            # Term-based retriever using match_phrase
            term_based_query = {
                "match_phrase": {
                    "text_entry": {
                        "query": query
                    }
                }
            }
            
            # Apply play filter and word count filter if needed
            filters = []
            if selected_plays:
                filters.append({"terms": {"play_name": selected_plays}})
            # filters.append(self._get_min_word_count_filter())
            
            if filters:
                term_based_query = {
                    "bool": {
                        "must": term_based_query,
                        "filter": {
                            "bool": {
                                "must": filters
                            }
                        }
                    }
                }
            
            retrievers.append({
                "standard": {
                    "query": term_based_query
                }
            })
            
            # Semantic retriever using semantic_text field
            semantic_query = {
                "semantic": {
                    "field": "text_entry_dense",
                    "query": query
                }
            }
            
            # Apply play filter and word count filter if needed
            filters = []
            if selected_plays:
                filters.append({"terms": {"play_name": selected_plays}})
            # filters.append(self._get_min_word_count_filter())
            
            if filters:
                semantic_query = {
                    "bool": {
                        "must": semantic_query,
                        "filter": {
                            "bool": {
                                "must": filters
                            }
                        }
                    }
                }
            
            retrievers.append({
                "standard": {
                    "query": semantic_query
                }
            })
            
            # Build the search body with RRF
            search_body = {
                "retriever": {
                    "rrf": {
                        "retrievers": retrievers,
                        "rank_constant": 60,
                        "rank_window_size": 100
                    }
                },
                "aggs": {
                    "plays": {
                        "terms": {
                            "field": "play_name",
                            "size": 50,
                            "order": {"_key": "asc"}
                        }
                    }
                },
                "from": from_,
                "size": size,
                "_source": ["play_name", "speaker", "text_entry", "line_id", "type"]
            }
            
            # Execute search on semantic index
            response = self.es.search(
                index="shakespeare-semantic",
                body=search_body
            )
        
        # Process results
        results = {
            "total": response["hits"]["total"]["value"] if "total" in response["hits"] else len(response["hits"]["hits"]),
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
                "score": hit.get("_score", 0),
                "rank": hit.get("_rank", None)  # RRF adds _rank field
            }
            results["hits"].append(result)
        
        # Process aggregations if available
        if "aggregations" in response and "plays" in response["aggregations"]:
            for bucket in response["aggregations"]["plays"]["buckets"]:
                results["aggregations"]["plays"].append({
                    "name": bucket["key"],
                    "count": bucket["doc_count"]
                })
        
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