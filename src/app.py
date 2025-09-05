from flask import Flask, render_template, request, jsonify
from search_backend import SearchBackend
import json

app = Flask(__name__, static_folder='static')

# Initialize search backend
try:
    search_backend = SearchBackend()
except Exception as e:
    print(f"Warning: Could not initialize search backend: {e}")
    search_backend = None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/search', methods=['POST'])
def search():
    """API endpoint for Shakespeare text search with faceting."""
    if not search_backend:
        return jsonify({"error": "Search backend not available"}), 503
    
    try:
        data = request.get_json()
        query = data.get('query', '')
        selected_plays = data.get('selected_plays', [])
        from_ = data.get('from', 0)
        size = data.get('size', 20)
        
        results = search_backend.search_shakespeare(
            query=query,
            selected_plays=selected_plays,
            from_=from_,
            size=size
        )
        
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search-semantic-sparse', methods=['POST'])
def search_semantic_sparse():
    """API endpoint for Shakespeare sparse vector semantic search with ELSER."""
    if not search_backend:
        return jsonify({"error": "Search backend not available"}), 503
    
    try:
        data = request.get_json()
        query = data.get('query', '')
        selected_plays = data.get('selected_plays', [])
        from_ = data.get('from', 0)
        size = data.get('size', 20)
        
        results = search_backend.search_shakespeare_semantic_sparse(
            query=query,
            selected_plays=selected_plays,
            from_=from_,
            size=size
        )
        
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search-semantic-dense', methods=['POST'])
def search_semantic_dense():
    """API endpoint for Shakespeare dense vector semantic search with E5."""
    if not search_backend:
        return jsonify({"error": "Search backend not available"}), 503
    
    try:
        data = request.get_json()
        query = data.get('query', '')
        selected_plays = data.get('selected_plays', [])
        from_ = data.get('from', 0)
        size = data.get('size', 20)
        
        results = search_backend.search_shakespeare_semantic_dense(
            query=query,
            selected_plays=selected_plays,
            from_=from_,
            size=size
        )
        
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search-hybrid', methods=['POST'])
def search_hybrid():
    """API endpoint for hybrid search using RRF."""
    if not search_backend:
        return jsonify({"error": "Search backend not available"}), 503
    
    try:
        data = request.get_json()
        query = data.get('query', '')
        selected_plays = data.get('selected_plays', [])
        from_ = data.get('from', 0)
        size = data.get('size', 20)
        
        results = search_backend.search_shakespeare_hybrid(
            query=query,
            selected_plays=selected_plays,
            from_=from_,
            size=size
        )
        
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/document/<int:line_id>')
def document_detail(line_id):
    """Display individual Elasticsearch document with nice formatting."""
    if not search_backend:
        return render_template('error.html', error="Search backend not available"), 503
    
    try:
        # Get the specific document
        document = search_backend.get_document_by_line_id(line_id)
        if not document:
            return render_template('error.html', error="Document not found"), 404
        
        return render_template('document_detail.html', document=document)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)