"""
RAG Chatbot backend using Claude and Elasticsearch.
"""

import os
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_elasticsearch import ElasticsearchStore
from langchain.schema import Document
from elasticsearch import Elasticsearch
import json

# Load environment variables
load_dotenv()

class ShakespeareChatbot:
    def __init__(self):
        """Initialize the Shakespeare RAG chatbot."""
        # Initialize Elasticsearch connection
        cloud_id = os.getenv('ELASTIC_CLOUD_ID')
        api_key = os.getenv('ELASTIC_API_KEY')
        anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')
        
        if not cloud_id or not api_key:
            raise ValueError("Please set ELASTIC_CLOUD_ID and ELASTIC_API_KEY in your .env file")
        
        if not anthropic_api_key:
            raise ValueError("Please set ANTHROPIC_API_KEY in your .env file")
        
        # Initialize Elasticsearch client
        self.es = Elasticsearch(
            cloud_id=cloud_id,
            api_key=api_key
        )
        
        if not self.es.ping():
            raise ConnectionError("Could not connect to Elasticsearch")
        
        # Initialize Claude
        self.llm = ChatAnthropic(
            model="claude-3-haiku-20240307",  # Fast and cost-effective for RAG
            anthropic_api_key=anthropic_api_key,
            temperature=0.7,
            max_tokens=1000
        )
        
        # System prompt for Shakespeare expert
        self.system_prompt = """You are a knowledgeable and friendly Shakespeare enthusiast who loves to discuss the Bard's works in a natural, conversational way. Think of yourself as that friend who studied English literature and can share fascinating insights about Shakespeare's plays and characters.

        Your style:
        - Write in a warm, engaging, conversational tone as if chatting with a friend
        - Share interesting observations and connections between themes, characters, or plays
        - Be enthusiastic about Shakespeare's genius but not pretentious
        - Use natural language - avoid overly academic jargon
        - Make the plays feel alive and relevant to modern readers

        Guidelines:
        1. Focus only on Shakespeare's works - if asked about other topics, gently redirect
        2. Base your insights on the provided context passages from the actual texts
        3. Always end your response with relevant direct quotes from the passages, introduced naturally
        4. When quoting, mention the play and speaker (e.g., "As Hamlet says..." or "In Macbeth, Lady Macbeth declares...")
        5. If the context doesn't contain relevant information, say so honestly
        6. Make Shakespeare accessible and interesting, not intimidating"""
        
        # Initialize conversation memory (will be stored in session)
        self.conversation_memory = []
    
    def search_shakespeare(self, query: str, size: int = 10) -> List[Dict[str, Any]]:
        """
        Search for relevant Shakespeare passages using hybrid RRF search with sparse vectors and field boosting.
        
        Args:
            query: The search query
            size: Number of results to return (default: 10 for RAG)
            
        Returns:
            List of relevant passages with metadata
        """
        # Build hybrid RRF search with sparse semantic + boosted term search
        search_body = {
            "retriever": {
                "rrf": {
                    "retrievers": [
                        # First retriever: Sparse vector semantic search (ELSER)
                        {
                            "standard": {
                                "query": {
                                    "sparse_vector": {
                                        "field": "text_entry_embedding",
                                        "inference_id": ".elser-2-elasticsearch", 
                                        "query": query
                                    }
                                }
                            }
                        },
                        # Second retriever: Boosted term search on play_name and speaker
                        {
                            "standard": {
                                "query": {
                                    "bool": {
                                        "should": [
                                            # Exact phrase match in text
                                            {
                                                "match_phrase": {
                                                    "text_entry": {
                                                        "query": query,
                                                        "boost": 1.0
                                                    }
                                                }
                                            },
                                            # Boosted matches in play name
                                            {
                                                "match": {
                                                    "play_name": {
                                                        "query": query,
                                                        "boost": 3.0
                                                    }
                                                }
                                            },
                                            # Highly boosted matches in speaker name
                                            {
                                                "match": {
                                                    "speaker": {
                                                        "query": query,
                                                        "boost": 5.0
                                                    }
                                                }
                                            },
                                            # General text match
                                            {
                                                "match": {
                                                    "text_entry": {
                                                        "query": query,
                                                        "operator": "or",
                                                        "minimum_should_match": "60%",
                                                        "boost": 1.0
                                                    }
                                                }
                                            }
                                        ],
                                        "minimum_should_match": 1
                                    }
                                }
                            }
                        }
                    ],
                    "rank_constant": 60,
                    "rank_window_size": 100
                }
            },
            "size": size,
            "_source": ["play_name", "speaker", "text_entry", "line_id", "type"]
        }
        
        try:
            response = self.es.search(
                index="shakespeare-semantic",
                body=search_body
            )
            
            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                results.append({
                    "play_name": source.get("play_name", ""),
                    "speaker": source.get("speaker", ""),
                    "text": source.get("text_entry", ""),
                    "score": hit.get("_score", 0),
                    "rank": hit.get("_rank", None)
                })
            
            return results
        except Exception as e:
            print(f"Hybrid search error: {e}")
            # Fallback to regular text search if hybrid search fails
            return self.fallback_search(query, size)
    
    def fallback_search(self, query: str, size: int = 5) -> List[Dict[str, Any]]:
        """
        Fallback to regular text search if semantic search is not available.
        """
        search_body = {
            "query": {
                "match": {
                    "text_entry": {
                        "query": query,
                        "operator": "or"
                    }
                }
            },
            "size": size,
            "_source": ["play_name", "speaker", "text_entry", "line_id", "type"]
        }
        
        try:
            response = self.es.search(
                index="shakespeare",
                body=search_body
            )
            
            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                results.append({
                    "play_name": source.get("play_name", ""),
                    "speaker": source.get("speaker", ""),
                    "text": source.get("text_entry", ""),
                    "score": hit["_score"]
                })
            
            return results
        except Exception as e:
            print(f"Fallback search error: {e}")
            return []
    
    def format_context(self, passages: List[Dict[str, Any]]) -> str:
        """
        Format retrieved passages into context for the LLM.
        
        Args:
            passages: List of passage dictionaries
            
        Returns:
            Formatted context string
        """
        if not passages:
            return "No relevant passages found in Shakespeare's works."
        
        context_parts = ["Here are relevant passages from Shakespeare's works:\n"]
        
        for i, passage in enumerate(passages, 1):
            context_parts.append(f"\n[Passage {i}]")
            context_parts.append(f"Play: {passage['play_name']}")
            if passage['speaker']:
                context_parts.append(f"Speaker: {passage['speaker']}")
            context_parts.append(f"Text: {passage['text']}")
            context_parts.append("---")
        
        return "\n".join(context_parts)
    
    def chat(self, user_message: str, conversation_history: List[Dict[str, str]] = None) -> str:
        """
        Process a chat message and return a response.
        
        Args:
            user_message: The user's question
            conversation_history: Previous conversation messages
            
        Returns:
            The chatbot's response
        """
        # Search for relevant passages
        passages = self.search_shakespeare(user_message)
        context = self.format_context(passages)
        
        # Build messages for Claude
        messages = [SystemMessage(content=self.system_prompt)]
        
        # Add conversation history if provided
        if conversation_history:
            for msg in conversation_history[-5:]:  # Keep last 5 exchanges for context
                if msg["role"] == "user":
                    messages.append(HumanMessage(content=msg["content"]))
                else:
                    messages.append(AIMessage(content=msg["content"]))
        
        # Create the prompt with context
        prompt_template = """Here are some passages from Shakespeare's works that might help answer the question:

{context}

Question: {question}

Please provide a natural, conversational response about Shakespeare's works based on these passages. Share your insights as if you're having a friendly discussion about literature. 

IMPORTANT: When you include ANY direct quotes from the passages, format them exactly like this:
<em>"quote text here"</em> (Play Name Act.Scene.Line)

For example:
<em>"To be or not to be, that is the question"</em> (Hamlet 3.1.64)

Cite ALL quotes throughout your response using this exact format - not just at the end. Every single quote should have its citation immediately following it. If these passages don't contain relevant information for the question, please say so."""
        
        # Add the current question with context
        current_prompt = prompt_template.format(
            context=context,
            question=user_message
        )
        messages.append(HumanMessage(content=current_prompt))
        
        try:
            # Get response from Claude
            response = self.llm.invoke(messages)
            return response.content
        except Exception as e:
            return f"I apologize, but I encountered an error: {str(e)}. Please try again."
    
    def stream_chat(self, user_message: str, conversation_history: List[Dict[str, str]] = None):
        """
        Stream a chat response for real-time display.
        
        Args:
            user_message: The user's question
            conversation_history: Previous conversation messages
            
        Yields:
            Chunks of the response
        """
        # Search for relevant passages
        passages = self.search_shakespeare(user_message)
        context = self.format_context(passages)
        
        # Build messages for Claude
        messages = [SystemMessage(content=self.system_prompt)]
        
        # Add conversation history if provided
        if conversation_history:
            for msg in conversation_history[-5:]:  # Keep last 5 exchanges for context
                if msg["role"] == "user":
                    messages.append(HumanMessage(content=msg["content"]))
                else:
                    messages.append(AIMessage(content=msg["content"]))
        
        # Create the prompt with context
        prompt_template = """Here are some passages from Shakespeare's works that might help answer the question:

{context}

Question: {question}

Please provide a natural, conversational response about Shakespeare's works based on these passages. Share your insights as if you're having a friendly discussion about literature. 

IMPORTANT: When you include ANY direct quotes from the passages, format them exactly like this:
<em>"quote text here"</em> (Play Name Act.Scene.Line)

For example:
<em>"To be or not to be, that is the question"</em> (Hamlet 3.1.64)

Cite ALL quotes throughout your response using this exact format - not just at the end. Every single quote should have its citation immediately following it. If these passages don't contain relevant information for the question, please say so."""
        
        # Add the current question with context
        current_prompt = prompt_template.format(
            context=context,
            question=user_message
        )
        messages.append(HumanMessage(content=current_prompt))
        
        try:
            # Stream response from Claude
            for chunk in self.llm.stream(messages):
                if chunk.content:
                    yield chunk.content
        except Exception as e:
            yield f"I apologize, but I encountered an error: {str(e)}. Please try again."