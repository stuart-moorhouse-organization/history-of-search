// Semantic Sparse Search (ELSER) Functions
let selectedSemanticSparsePlays = [];
let semanticSparseCodeViewVisible = false;
let lastSemanticSparseQuery = null;

function toggleSemanticSparseCodeView() {
    const codeDisplay = document.getElementById('semantic-sparse-code-display');
    const toggleBtn = document.getElementById('semantic-sparse-code-toggle');
    
    if (!codeDisplay || !toggleBtn) return;
    
    semanticSparseCodeViewVisible = !semanticSparseCodeViewVisible;
    
    if (semanticSparseCodeViewVisible) {
        codeDisplay.classList.add('visible');
        toggleBtn.classList.add('active');
        toggleBtn.textContent = '</code>';
    } else {
        codeDisplay.classList.remove('visible');
        toggleBtn.classList.remove('active');
        toggleBtn.textContent = '<code>';
    }
}

async function performSemanticSparseSearch() {
    const query = document.getElementById('semantic-sparse-search-box').value.trim();
    const resultsArea = document.getElementById('semantic-sparse-results');
    
    resultsArea.innerHTML = '<div class="loading">Searching with sparse vectors (ELSER)...</div>';
    
    try {
        const searchRequest = {
            query: query,
            selected_plays: selectedSemanticSparsePlays,
            from: 0,
            size: 20
        };
        
        lastSemanticSparseQuery = searchRequest;
        updateSemanticSparseCodeDisplay(searchRequest, null, null);
        
        const response = await fetch('/api/search-semantic-sparse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(searchRequest)
        });
        
        const data = await response.json();
        
        if (response.ok) {
            displaySemanticSparseResults(data);
            updateSemanticSparseFacets(data.aggregations.plays);
            updateSemanticSparseCodeDisplay(searchRequest, data.elasticsearch_query, data.first_hit_embedding);
        } else {
            resultsArea.innerHTML = `<p>Error: ${data.error}</p>`;
        }
    } catch (error) {
        resultsArea.innerHTML = `<p>Error: ${error.message}</p>`;
    }
}

function displaySemanticSparseResults(data) {
    const resultsArea = document.getElementById('semantic-sparse-results');
    
    if (data.total === 0) {
        resultsArea.innerHTML = '<p>No results found.</p>';
        return;
    }
    
    let html = `<div class="search-stats">Found ${data.total.toLocaleString()} results</div>`;
    
    data.hits.forEach(hit => {
        const highlightText = hit.highlight && hit.highlight.length > 0 ? hit.highlight[0] : hit.text_entry;
        html += `
            <div class="search-result" onclick="openDocument(${hit.line_id})" title="Click to view document">
                <div class="result-meta">
                    <strong>${hit.play_name}</strong>
                    ${hit.speaker ? ` - ${hit.speaker}` : ''}
                    (${hit.type})
                    <span style="color: #888; font-size: 11px; margin-left: 10px;">🔍 Click to view document</span>
                </div>
                <div class="result-text">${highlightText}</div>
            </div>
        `;
    });
    
    resultsArea.innerHTML = html;
}

function updateSemanticSparseFacets(plays) {
    const facetsContainer = document.getElementById('semantic-sparse-play-facets');
    let html = '';
    plays.forEach(play => {
        const checked = selectedSemanticSparsePlays.includes(play.name) ? 'checked' : '';
        html += `
            <div class="facet-item" onclick="toggleSemanticSparsePlay('${play.name}')">
                <input type="checkbox" class="facet-checkbox" ${checked}>
                <span class="facet-label">${play.name}</span>
                <span class="facet-count">(${play.count})</span>
            </div>
        `;
    });
    facetsContainer.innerHTML = html;
}

function toggleSemanticSparsePlay(playName) {
    const index = selectedSemanticSparsePlays.indexOf(playName);
    if (index > -1) {
        selectedSemanticSparsePlays.splice(index, 1);
    } else {
        selectedSemanticSparsePlays.push(playName);
    }
    
    const event = window.event;
    const checkbox = event.target.type === 'checkbox' ? event.target : event.currentTarget.querySelector('.facet-checkbox');
    if (checkbox) {
        checkbox.checked = selectedSemanticSparsePlays.includes(playName);
    }
    
    performSemanticSparseSearch();
}

function updateSemanticSparseCodeDisplay(request, esQuery, firstHitEmbedding) {
    const codeElement = document.getElementById('semantic-sparse-es-query-display');
    if (codeElement) {
        let displayContent = '';
        
        // Display the Elasticsearch query
        if (esQuery) {
            displayContent += '<div style="margin-bottom: 20px;"><strong>Elasticsearch Query:</strong></div>';
            if (typeof formatJSON === 'function') {
                displayContent += formatJSON(esQuery);
            } else {
                displayContent += `<pre>${JSON.stringify(esQuery, null, 2)}</pre>`;
            }
        }
        
        // Display the first hit's embedding if available
        if (firstHitEmbedding) {
            displayContent += '<div style="margin-top: 30px; margin-bottom: 20px;"><strong>First Result\'s ELSER Embedding (text_entry_embedding):</strong></div>';
            displayContent += '<div style="max-height: 300px; overflow-y: auto; background: #f5f5f5; padding: 10px; border-radius: 4px; border: 1px solid #ddd;">';
            
            // Sort the embedding tokens by weight (descending) and show top tokens
            const tokens = Object.entries(firstHitEmbedding)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 50); // Show top 50 tokens
            
            displayContent += '<pre style="margin: 0; color: #1a1a1a; font-weight: 500; font-size: 13px; line-height: 1.6;">{';
            tokens.forEach(([token, weight], index) => {
                displayContent += `\n  <span style="color: #0969da;">"${token}"</span>: <span style="color: #d1242f; font-weight: bold;">${weight.toFixed(6)}</span>${index < tokens.length - 1 ? ',' : ''}`;
            });
            if (Object.keys(firstHitEmbedding).length > 50) {
                displayContent += '\n  <span style="color: #6e7781; font-style: italic;">... (and ' + (Object.keys(firstHitEmbedding).length - 50) + ' more tokens)</span>';
            }
            displayContent += '\n}</pre></div>';
        }
        
        codeElement.innerHTML = displayContent;
    }
}

// Semantic Dense Search (E5) Functions
let selectedSemanticDensePlays = [];
let semanticDenseCodeViewVisible = false;
let lastSemanticDenseQuery = null;

function toggleSemanticDenseCodeView() {
    const codeDisplay = document.getElementById('semantic-dense-code-display');
    const toggleBtn = document.getElementById('semantic-dense-code-toggle');
    
    if (!codeDisplay || !toggleBtn) return;
    
    semanticDenseCodeViewVisible = !semanticDenseCodeViewVisible;
    
    if (semanticDenseCodeViewVisible) {
        codeDisplay.classList.add('visible');
        toggleBtn.classList.add('active');
        toggleBtn.textContent = '</code>';
    } else {
        codeDisplay.classList.remove('visible');
        toggleBtn.classList.remove('active');
        toggleBtn.textContent = '<code>';
    }
}

async function performSemanticDenseSearch() {
    const query = document.getElementById('semantic-dense-search-box').value.trim();
    const resultsArea = document.getElementById('semantic-dense-results');
    
    resultsArea.innerHTML = '<div class="loading">Searching with dense vectors (E5)...</div>';
    
    try {
        const searchRequest = {
            query: query,
            selected_plays: selectedSemanticDensePlays,
            from: 0,
            size: 20
        };
        
        lastSemanticDenseQuery = searchRequest;
        updateSemanticDenseCodeDisplay(searchRequest);
        
        const response = await fetch('/api/search-semantic-dense', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(searchRequest)
        });
        
        const data = await response.json();
        
        if (response.ok) {
            displaySemanticDenseResults(data);
            updateSemanticDenseFacets(data.aggregations.plays);
            updateSemanticDenseCodeDisplay(searchRequest, data.elasticsearch_query);
        } else {
            resultsArea.innerHTML = `<p>Error: ${data.error}</p>`;
        }
    } catch (error) {
        resultsArea.innerHTML = `<p>Error: ${error.message}</p>`;
    }
}

function displaySemanticDenseResults(data) {
    const resultsArea = document.getElementById('semantic-dense-results');
    
    if (data.total === 0) {
        resultsArea.innerHTML = '<p>No results found.</p>';
        return;
    }
    
    let html = `<div class="search-stats">Found ${data.total.toLocaleString()} results</div>`;
    
    data.hits.forEach(hit => {
        const highlightText = hit.highlight && hit.highlight.length > 0 ? hit.highlight[0] : hit.text_entry;
        html += `
            <div class="search-result" onclick="openDocument(${hit.line_id})" title="Click to view document">
                <div class="result-meta">
                    <strong>${hit.play_name}</strong>
                    ${hit.speaker ? ` - ${hit.speaker}` : ''}
                    (${hit.type})
                    <span style="color: #888; font-size: 11px; margin-left: 10px;">🔍 Click to view document</span>
                </div>
                <div class="result-text">${highlightText}</div>
            </div>
        `;
    });
    
    resultsArea.innerHTML = html;
}

function updateSemanticDenseFacets(plays) {
    const facetsContainer = document.getElementById('semantic-dense-play-facets');
    let html = '';
    plays.forEach(play => {
        const checked = selectedSemanticDensePlays.includes(play.name) ? 'checked' : '';
        html += `
            <div class="facet-item" onclick="toggleSemanticDensePlay('${play.name}')">
                <input type="checkbox" class="facet-checkbox" ${checked}>
                <span class="facet-label">${play.name}</span>
                <span class="facet-count">(${play.count})</span>
            </div>
        `;
    });
    facetsContainer.innerHTML = html;
}

function toggleSemanticDensePlay(playName) {
    const index = selectedSemanticDensePlays.indexOf(playName);
    if (index > -1) {
        selectedSemanticDensePlays.splice(index, 1);
    } else {
        selectedSemanticDensePlays.push(playName);
    }
    
    const event = window.event;
    const checkbox = event.target.type === 'checkbox' ? event.target : event.currentTarget.querySelector('.facet-checkbox');
    if (checkbox) {
        checkbox.checked = selectedSemanticDensePlays.includes(playName);
    }
    
    performSemanticDenseSearch();
}

function updateSemanticDenseCodeDisplay(request, esQuery) {
    const codeElement = document.getElementById('semantic-dense-es-query-display');
    if (codeElement) {
        if (esQuery) {
            // Use formatJSON if available, otherwise fallback to plain JSON
            if (typeof formatJSON === 'function') {
                codeElement.innerHTML = formatJSON(esQuery);
            } else {
                codeElement.innerHTML = `<pre>${JSON.stringify(esQuery, null, 2)}</pre>`;
            }
        } else if (request) {
            if (typeof formatJSON === 'function') {
                codeElement.innerHTML = formatJSON(request);
            } else {
                codeElement.innerHTML = `<pre>${JSON.stringify(request, null, 2)}</pre>`;
            }
        }
    }
}

// Initialize event listeners when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    // Semantic Sparse Search event listeners
    const semanticSparseSearchBtn = document.getElementById('semantic-sparse-search-btn');
    const semanticSparseSearchBox = document.getElementById('semantic-sparse-search-box');
    
    if (semanticSparseSearchBtn) {
        semanticSparseSearchBtn.addEventListener('click', performSemanticSparseSearch);
    }
    
    if (semanticSparseSearchBox) {
        semanticSparseSearchBox.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                performSemanticSparseSearch();
            }
        });
    }
    
    // Semantic Dense Search event listeners
    const semanticDenseSearchBtn = document.getElementById('semantic-dense-search-btn');
    const semanticDenseSearchBox = document.getElementById('semantic-dense-search-box');
    
    if (semanticDenseSearchBtn) {
        semanticDenseSearchBtn.addEventListener('click', performSemanticDenseSearch);
    }
    
    if (semanticDenseSearchBox) {
        semanticDenseSearchBox.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                performSemanticDenseSearch();
            }
        });
    }
});