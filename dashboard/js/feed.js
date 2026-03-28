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

    // --- MOCK API POLLING ---
    // Instead of actual fetch, we simulate incoming streams.
    
    const names = ['rahul', 'priya', 'amit', 'neha', 'vikram', 'anita'];
    function randUpi() { return names[Math.floor(Math.random() * names.length)] + Math.floor(Math.random()*99) + '@ybl'; }
    
    function fetchLiveTransactions() {
        // Simulate normal transactions ticking in
        const count = Math.floor(Math.random() * 3) + 1; // 1 to 3 per poll
        for (let i=0; i<count; i++) {
            const txn = {
                timestamp: Date.now() - Math.random() * 1000,
                sender: randUpi(),
                receiver: randUpi(),
                amount: Math.floor(Math.random() * 5000) + 100,
                flagged: false
            };
            addTransactionToFeed(txn);
            
            // Dispatch event for D3 graph to consume
            window.dispatchEvent(new CustomEvent('kavach:new_txn', { detail: txn }));
        }
    }

    // Poll every 2 seconds
    setInterval(fetchLiveTransactions, 2000);

    // Provide some immediate data
    setTimeout(fetchLiveTransactions, 500);

    // --- SIMULATE SCAM BUTTON ---
    simulateBtn.addEventListener('click', () => {
        simulateBtn.disabled = true;
        simulateBtn.textContent = 'Simulating...';
        simulateBtn.classList.remove('btn-outline');
        simulateBtn.classList.add('btn-primary');
        
        const victim = `victim_${Math.floor(Math.random()*99)}@sbi`;
        const mules = [randUpi(), randUpi(), randUpi()];
        const amounts = [100000, 50000, 200000];
        
        let step = 0;
        const interval = setInterval(() => {
            if (step >= mules.length) {
                clearInterval(interval);
                simulateBtn.disabled = false;
                simulateBtn.textContent = 'Simulate Scam';
                simulateBtn.classList.add('btn-outline');
                simulateBtn.classList.remove('btn-primary');
                
                // Trigger alert
                addAlertToFeed(`Model FL_XGB_94 flagged 3 rapid high-value transfers (Total: ₹3,50,000) from ${victim} to newly active accounts. Confidence: 96.8%.`);
                return;
            }
            
            const txn = {
                timestamp: Date.now(),
                sender: victim,
                receiver: mules[step],
                amount: amounts[step],
                flagged: true,
                is_scam: true
            };
            
            addTransactionToFeed(txn);
            window.dispatchEvent(new CustomEvent('kavach:new_txn', { detail: txn }));
            step++;
            
        }, 800); // Send one flagged txn every 0.8s for visual effect
    });
});
