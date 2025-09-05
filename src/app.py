from flask import Flask, render_template, request, jsonify
from search_backend import SearchBackend
import json

app = Flask(__name__)

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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)