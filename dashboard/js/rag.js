// ===== js/rag.js ===== //

document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.getElementById('rag-search-input');
    const searchBtn = document.getElementById('btn-rag-search');
    const resultsContainer = document.getElementById('rag-results-container');

    const MOCK_RESULTS = [
        {
            id: 'RAG_CMP_0102',
            score: '0.94',
            amount: 750000,
            narrative: "...individuals dressed in official-looking uniforms sitting in a room that closely resembled a real police station, complete with national emblems and official insignia. They declared that I was under 'digital arrest' and strictly prohibited me from muting the microphone, turning off the camera...",
            tags: ['digital_arrest', 'impersonation', 'cbi', 'skype_call', 'coercion']
        },
        {
            id: 'RAG_CMP_0045',
            score: '0.88',
            amount: 250000,
            narrative: "...received an unexpected phone call at around 14:00. The caller identified themselves as a senior investigating officer from the Customs. The person sounded extremely authoritative and informed me that my Aadhaar card and PAN card details had been found...",
            tags: ['digital_arrest', 'impersonation', 'customs', 'coercion']
        },
        {
            id: 'RAG_CMP_0188',
            score: '0.82',
            amount: 1500000,
            narrative: "...They transferred my call to what he described as the 'cyber crime investigation branch'. I was then manipulated into logging on to a WhatsApp video call for formal questioning. The individuals on the screen were wearing uniforms and sitting behind heavy wooden desks...",
            tags: ['digital_arrest', 'impersonation', 'whatsapp_call', 'coercion']
        }
    ];

    function renderResults(results) {
        resultsContainer.innerHTML = '';
        
        if (results.length === 0) {
            resultsContainer.innerHTML = `<div class="placeholder-text text-muted">No semantically similar cases found.</div>`;
            return;
        }

        results.forEach(res => {
            const card = document.createElement('div');
            card.className = 'rag-card';
            
            // The backend returns city, amount_lost, similarity, narrative, complaint_id
            card.innerHTML = `
                <div class="rag-card-header">
                    <span style="color:var(--text-muted); font-family:monospace;">${res.complaint_id}</span>
                    <span class="score-badge">Similarity: ${res.similarity.toFixed(3)}</span>
                </div>
                <div class="rag-narrative">
                    "${res.narrative}"
                </div>
                <div style="display:flex; justify-content:space-between; align-items:flex-end;">
                    <div style="color:var(--accent-red); font-weight:700;">Loss: ₹${res.amount_lost.toLocaleString()}</div>
                    <div style="max-width:60%; text-align:right;"><span style="color:var(--text-muted); font-size:0.85rem;">📍 ${res.city}</span></div>
                </div>
            `;
            resultsContainer.appendChild(card);
        });
    }

    async function handleSearch() {
        const query = searchInput.value.trim();
        if (!query) return;

        // Show loading state
        resultsContainer.innerHTML = `<div class="placeholder-text">Searching Vector Database... <br><small class="text-muted">(Using actual FAISS backend)</small></div>`;

        searchBtn.disabled = true;
        searchBtn.textContent = 'Searching...';

        try {
            const res = await fetch('http://localhost:5000/api/rag/search', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query })
            });
            
            if (res.ok) {
                const data = await res.json();
                renderResults(data.results || []);
            } else {
                resultsContainer.innerHTML = `<div class="placeholder-text text-muted" style="color:var(--accent-red) !important;">Error connecting to backend vector store.</div>`;
            }
        } catch (e) {
            console.error(e);
            resultsContainer.innerHTML = `<div class="placeholder-text text-muted" style="color:var(--accent-red) !important;">Backend is offline. Ensure app.py is running.</div>`;
        } finally {
            searchBtn.disabled = false;
            searchBtn.textContent = 'Search DB';
        }
    }

    searchBtn.addEventListener('click', handleSearch);
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleSearch();
    });
});
