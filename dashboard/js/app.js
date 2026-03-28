// ===== js/app.js ===== //

document.addEventListener('DOMContentLoaded', () => {
    // 1. Navigation Logic
    const navItems = document.querySelectorAll('.nav-item');
    const sections = document.querySelectorAll('.view-section');

    function switchSection(targetId) {
        // Update Nav
        navItems.forEach(item => {
            if (item.dataset.target === targetId) {
                item.classList.add('active');
            } else {
                item.classList.remove('active');
            }
        });

        // Update Sections
        sections.forEach(section => {
            if (section.id === targetId) {
                section.classList.add('active');
            } else {
                section.classList.remove('active');
            }
        });
        
        // Trigger resize events for elements that need it (like D3 graph)
        if (targetId === 'monitoring') {
            window.dispatchEvent(new Event('resize'));
        }
    }

    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            switchSection(e.target.dataset.target);
        });
    });

    // Landing Page Monitor Button
    document.getElementById('btn-enter-monitor').addEventListener('click', () => {
        switchSection('monitoring');
    });

    // 2. Animated Threat Counter (₹22L / hour ticking up)
    const threatCounterEl = document.getElementById('threat-counter');
    let currentAmount = 2200000;
    
    // Add approx ₹611 every second (22L / 3600s)
    setInterval(() => {
        currentAmount += Math.floor(Math.random() * 800) + 200;
        threatCounterEl.textContent = '₹' + currentAmount.toLocaleString('en-IN');
    }, 1000);
    
    console.log("Kavach Dashboard Initialized.");
});
