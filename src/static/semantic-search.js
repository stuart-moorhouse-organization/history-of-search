// Semantic Sparse Search (ELSER) Functions
let selectedSemanticSparsePlays = [];
let semanticSparseCodeViewVisible = false;
let lastSemanticSparseQuery = null;

function toggleSemanticSparseCodeView() {
    const codeDisplay = document.getElementById('semantic-sparse-code-display');
    const toggleBtn = document.getElementById('semantic-sparse-code-toggle');
    
    semanticSparseCodeViewVisible = !semanticSparseCodeViewVisible;
    
    if (semanticSparseCodeViewVisible) {
        codeDisplay.style.display = 'block';
        toggleBtn.style.background = '#3498db';
    } else {
        codeDisplay.style.display = 'none';
        toggleBtn.style.background = '#2c3e50';
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
        updateSemanticSparseCodeDisplay(searchRequest);
        
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
            updateSemanticSparseCodeDisplay(searchRequest, data.elasticsearch_query);
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
        resultsArea.innerHTML = '<p>No sparse vector matches found.</p>';
        return;
    }
    
    let html = `<div class="search-stats">Found ${data.total.toLocaleString()} sparse vector matches</div>`;
    
    data.hits.forEach(hit => {
        const highlightText = hit.highlight && hit.highlight.length > 0 ? hit.highlight[0] : hit.text_entry;
        html += `
            <div class="search-result">
                <a href="/document/${hit.line_id}" class="result-link">
                    <div class="result-play">${hit.play_name}</div>
                    <div class="result-speaker">${hit.speaker || 'Narrative'}</div>
                    <div class="result-text">${highlightText}</div>
                </a>
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

function updateSemanticSparseCodeDisplay(request, esQuery) {
    const codeElement = document.getElementById('semantic-sparse-es-query-display');
    if (esQuery) {
        codeElement.innerHTML = `<pre>${JSON.stringify(esQuery, null, 2)}</pre>`;
    }
}

// Semantic Dense Search (E5) Functions
let selectedSemanticDensePlays = [];
let semanticDenseCodeViewVisible = false;
let lastSemanticDenseQuery = null;

function toggleSemanticDenseCodeView() {
    const codeDisplay = document.getElementById('semantic-dense-code-display');
    const toggleBtn = document.getElementById('semantic-dense-code-toggle');
    
    semanticDenseCodeViewVisible = !semanticDenseCodeViewVisible;
    
    if (semanticDenseCodeViewVisible) {
        codeDisplay.style.display = 'block';
        toggleBtn.style.background = '#3498db';
    } else {
        codeDisplay.style.display = 'none';
        toggleBtn.style.background = '#2c3e50';
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
        resultsArea.innerHTML = '<p>No dense vector matches found.</p>';
        return;
    }
    
    let html = `<div class="search-stats">Found ${data.total.toLocaleString()} dense vector matches</div>`;
    
    data.hits.forEach(hit => {
        const highlightText = hit.highlight && hit.highlight.length > 0 ? hit.highlight[0] : hit.text_entry;
        html += `
            <div class="search-result">
                <a href="/document/${hit.line_id}" class="result-link">
                    <div class="result-play">${hit.play_name}</div>
                    <div class="result-speaker">${hit.speaker || 'Narrative'}</div>
                    <div class="result-text">${highlightText}</div>
                </a>
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
    if (esQuery) {
        codeElement.innerHTML = `<pre>${JSON.stringify(esQuery, null, 2)}</pre>`;
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