from flask import Flask, render_template, request, jsonify, session, Response, stream_with_context
from search_backend import SearchBackend
from chatbot import ShakespeareChatbot
import json
import secrets

app = Flask(__name__, static_folder='static')
app.secret_key = secrets.token_hex(32)  # For session management

# Initialize search backend
try:
    search_backend = SearchBackend()
except Exception as e:
    print(f"Warning: Could not initialize search backend: {e}")
    search_backend = None

# Initialize chatbot
try:
    chatbot = ShakespeareChatbot()
except Exception as e:
    print(f"Warning: Could not initialize chatbot: {e}")
    chatbot = None

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

@app.route('/api/chat', methods=['POST'])
def chat():
    """Chat endpoint for Shakespeare chatbot."""
    if not chatbot:
        return jsonify({"error": "Chatbot not available"}), 503
    
    try:
        data = request.get_json()
        message = data.get('message', '')
        
        if not message:
            return jsonify({"error": "No message provided"}), 400
        
        # Get conversation history from session
        if 'conversation_history' not in session:
            session['conversation_history'] = []
        
        conversation_history = session['conversation_history']
        
        # Get response from chatbot
        response = chatbot.chat(message, conversation_history)
        
        # Update conversation history
        conversation_history.append({"role": "user", "content": message})
        conversation_history.append({"role": "assistant", "content": response})
        
        # Keep only last 10 exchanges (20 messages)
        if len(conversation_history) > 20:
            conversation_history = conversation_history[-20:]
        
        session['conversation_history'] = conversation_history
        session.modified = True
        
        return jsonify({
            "response": response,
            "conversation_id": session.get('conversation_id', 'default')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat/stream', methods=['POST'])
def chat_stream():
    """Streaming chat endpoint for real-time responses."""
    if not chatbot:
        return jsonify({"error": "Chatbot not available"}), 503
    
    try:
        data = request.get_json()
        message = data.get('message', '')
        
        if not message:
            return jsonify({"error": "No message provided"}), 400
        
        # Get conversation history from session
        if 'conversation_history' not in session:
            session['conversation_history'] = []
        
        conversation_history = session['conversation_history']
        
        def generate():
            full_response = []
            for chunk in chatbot.stream_chat(message, conversation_history):
                full_response.append(chunk)
                yield f"data: {json.dumps({'chunk': chunk})}\n\n"
            
            # Update conversation history after streaming completes
            conversation_history.append({"role": "user", "content": message})
            conversation_history.append({"role": "assistant", "content": ''.join(full_response)})
            
            # Keep only last 10 exchanges
            if len(conversation_history) > 20:
                session['conversation_history'] = conversation_history[-20:]
            else:
                session['conversation_history'] = conversation_history
            
            session.modified = True
            yield f"data: {json.dumps({'done': True})}\n\n"
        
        return Response(
            stream_with_context(generate()),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no'
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat/clear', methods=['POST'])
def clear_chat():
    """Clear conversation history."""
    session['conversation_history'] = []
    session.modified = True
    return jsonify({"message": "Conversation history cleared"})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)