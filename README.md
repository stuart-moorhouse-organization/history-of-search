# Shakespeare Search Lab

Interactive demonstration of search evolution using Shakespeare's complete works as a dataset. Compare five different search approaches side-by-side to understand their strengths and limitations.

## Search Experiences

1. **Term-Based Search** - Traditional keyword matching
2. **Semantic Search (Sparse Vector)** - ELSER learned sparse retrieval  
3. **Semantic Search (Dense Vector)** - E5 multilingual dense embeddings with KNN
4. **Hybrid Search** - Combines term and semantic search using Reciprocal Rank Fusion (RRF)
5. **Chat Search** - RAG-powered conversational search with Claude

## Demo Queries

Test these queries to see how different search methods perform:

- **"roses"** - Exact term matching works well
- **"a quote about flowers"** - Semantic understanding shines
- **"sound and fury"** - Famous phrase (hybrid excels)
- **"mortal coil"** - Another famous phrase 
- **"brief candle"** - Metaphorical content

## Quick Start

```bash
# Run the Flask app
cd src
python app.py

# Access at http://localhost:5000
```

## Requirements

- Elasticsearch Cloud deployment with ML capabilities
- Python 3.8+
- Environment variables in `.env` file