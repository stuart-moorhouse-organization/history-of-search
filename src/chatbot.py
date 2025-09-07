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
        self.system_prompt = """You are a Shakespeare expert assistant. Your role is to answer questions about Shakespeare's works based on the provided context from his plays and sonnets.

        Guidelines:
        1. Only answer questions related to Shakespeare's works
        2. Base your answers on the provided context passages
        3. If the context doesn't contain relevant information, say so
        4. Always cite the specific play and speaker when referencing the text
        5. Be conversational but authoritative
        6. Keep answers concise but informative
        7. If asked about non-Shakespeare topics, politely redirect to Shakespeare

        You have access to the complete works of Shakespeare and should provide accurate, helpful responses."""
        
        # Initialize conversation memory (will be stored in session)
        self.conversation_memory = []
    
    def search_shakespeare(self, query: str, size: int = 5) -> List[Dict[str, Any]]:
        """
        Search for relevant Shakespeare passages using semantic search.
        
        Args:
            query: The search query
            size: Number of results to return
            
        Returns:
            List of relevant passages with metadata
        """
        search_body = {
            "query": {
                "semantic": {
                    "field": "text_entry_dense",
                    "query": query
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
                    "score": hit["_score"]
                })
            
            return results
        except Exception as e:
            print(f"Search error: {e}")
            # Fallback to regular text search if semantic search fails
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
        prompt_template = """Based on the following context from Shakespeare's works, please answer the question.

Context:
{context}

Question: {question}

Please provide a helpful and accurate answer based on the context provided. If the context doesn't contain relevant information, please say so."""
        
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
        prompt_template = """Based on the following context from Shakespeare's works, please answer the question.

Context:
{context}

Question: {question}

Please provide a helpful and accurate answer based on the context provided. If the context doesn't contain relevant information, please say so."""
        
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