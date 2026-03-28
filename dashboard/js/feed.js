// ===== js/feed.js ===== //

document.addEventListener('DOMContentLoaded', () => {
    const feedList = document.getElementById('transaction-feed');
    const alertList = document.getElementById('alert-feed');
    const simulateBtn = document.getElementById('btn-simulate');
    
    // Maintain rolling list to prevent unbounded DOM growth
    const MAX_FEED_ITEMS = 50;
    const MAX_ALERTS = 5;

    // Formatter
    const currencyFmt = new Intl.NumberFormat('en-IN', {
        style: 'currency',
        currency: 'INR',
        maximumFractionDigits: 0
    });

    // Create a feed row
    function createFeedRow(txn) {
        const timeStr = new Date(txn.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second:'2-digit' });
        
        const row = document.createElement('div');
        row.className = `feed-item ${txn.flagged ? 'flagged' : 'normal'}`;
        
        row.innerHTML = `
            <span>${timeStr}</span>
            <span class="feed-account">${txn.sender.substring(0,8)}... → ${txn.receiver.substring(0,8)}...</span>
            <span class="feed-amt ${txn.flagged ? 'value-amber' : ''}">${currencyFmt.format(txn.amount)}</span>
        `;
        return row;
    }

    // Add row to feed container
    function addTransactionToFeed(txn) {
        // remove empty state
        const emptyState = feedList.querySelector('.feed-empty');
        if (emptyState) emptyState.remove();

        const row = createFeedRow(txn);
        feedList.insertBefore(row, feedList.firstChild);

        // cleanup old
        while (feedList.childElementCount > MAX_FEED_ITEMS) {
            feedList.removeChild(feedList.lastChild);
        }
    }

    // Create an alert row
    function createAlertRow(msg) {
        const row = document.createElement('div');
        row.className = 'alert-item';
        
        const timeStr = new Date().toLocaleTimeString();
        row.innerHTML = `
            <strong>[${timeStr}] SCAM EPISODE DETECTED</strong><br>
            <span style="color:var(--text-muted); font-size: 0.9rem; margin-top:5px; display:inline-block;">
                ${msg}
            </span>
        `;
        return row;
    }

    function addAlertToFeed(msg) {
        const emptyState = alertList.querySelector('.empty-alert');
        if (emptyState) emptyState.remove();

        const row = createAlertRow(msg);
        alertList.insertBefore(row, alertList.firstChild);

        while (alertList.childElementCount > MAX_ALERTS) {
            alertList.removeChild(alertList.lastChild);
        }
    }

    // --- FULL STACK API POLLING ---
    let lastSeenTxnIds = new Set();
    let lastSeenAlertIds = new Set();
    const API_BASE = '/api';

    async function fetchLiveTransactions() {
        try {
            const res = await fetch(`${API_BASE}/transactions`);
            if (!res.ok) return;
            const txns = await res.json();
            
            // Filter new ones
            const newTxns = txns.filter(t => !lastSeenTxnIds.has(t.txn_id));
            if (newTxns.length === 0) return;
            
            newTxns.forEach(txn => {
                lastSeenTxnIds.add(txn.txn_id);
                // Keep set size manageable
                if (lastSeenTxnIds.size > 1000) {
                    const iterator = lastSeenTxnIds.values();
                    lastSeenTxnIds.delete(iterator.next().value);
                }
                
                // Add to UI
                addTransactionToFeed({
                    timestamp: txn.timestamp,
                    sender: txn.sender,
                    receiver: txn.receiver,
                    amount: txn.amount,
                    flagged: txn.is_flagged,
                    is_scam: txn.is_flagged
                });
                
                // Dispatch for D3 graph
                window.dispatchEvent(new CustomEvent('kavach:new_txn', { detail: txn }));
            });
        } catch (e) {
            console.error("Backend offline or error fetching txns:", e);
        }
    }

    async function fetchAlerts() {
        try {
            const res = await fetch(`${API_BASE}/alerts`);
            if (!res.ok) return;
            const alerts = await res.json();
            
            // Start from end to prepend in order, or just filter new
            const newAlerts = alerts.filter(a => !lastSeenAlertIds.has(a.alert_id));
            newAlerts.forEach(a => {
                lastSeenAlertIds.add(a.alert_id);
                addAlertToFeed(a.pattern_description);
            });
        } catch (e) {
            console.error("Error fetching alerts:", e);
        }
    }

    // Poll every 2 seconds
    setInterval(() => {
        fetchLiveTransactions();
        fetchAlerts();
    }, 2000);

    // Initial fetch
    fetchLiveTransactions();
    fetchAlerts();

    // --- SIMULATE SCAM BUTTON ---
    simulateBtn.addEventListener('click', async () => {
        simulateBtn.disabled = true;
        simulateBtn.textContent = 'Simulating...';
        simulateBtn.classList.remove('btn-outline');
        simulateBtn.classList.add('btn-primary');
        
        try {
            const res = await fetch(`${API_BASE}/simulate-scam`, { method: 'POST' });
            if (res.ok) {
                console.log("Scam triggered successfully.");
            }
        } catch (e) {
            console.error("Error triggering scam:", e);
        } finally {
            setTimeout(() => {
                simulateBtn.disabled = false;
                simulateBtn.textContent = 'Simulate Scam';
                simulateBtn.classList.add('btn-outline');
                simulateBtn.classList.remove('btn-primary');
            }, 3000); // 3 sec cooldown to let txns flow in
        }
    });
});
