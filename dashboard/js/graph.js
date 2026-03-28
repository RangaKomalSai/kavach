// ===== js/graph.js ===== //

document.addEventListener('DOMContentLoaded', () => {
    const container = document.getElementById('graph-canvas');
    let width = container.clientWidth || 800;
    let height = container.clientHeight || 500;

    const svg = d3.select('#graph-canvas')
        .append('svg')
        .attr('width', '100%')
        .attr('height', '100%');

    const g = svg.append('g');

    // Zoom setup
    const zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (e) => g.attr('transform', e.transform));
    svg.call(zoom);

    // Initial state: blank graph
    let nodesMap = new Map();
    let linksMap = new Map();
    
    // We'll keep the D3 arrays up-to-date
    let d3Nodes = [];
    let d3Links = [];

    // Force simulation
    const simulation = d3.forceSimulation(d3Nodes)
        .force('link', d3.forceLink(d3Links).id(d => d.id).distance(80))
        .force('charge', d3.forceManyBody().strength(-150))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('x', d3.forceX(width / 2).strength(0.05))
        .force('y', d3.forceY(height / 2).strength(0.05));

    // Tooltip
    const tooltip = d3.select('body').append('div')
        .attr('class', 'd3-tooltip');

    // D3 update function
    let linkElements = g.append("g").attr("class", "links").selectAll(".link");
    let nodeElements = g.append("g").attr("class", "nodes").selectAll(".node");

    function updateGraph() {
        // Links
        linkElements = linkElements.data(d3Links, d => `${d.source.id || d.source}_${d.target.id || d.target}`);
        linkElements.exit().remove();
        
        const linkEnter = linkElements.enter().append("line")
            .attr("class", "link")
            .style("stroke-width", d => Math.max(1, Math.min(10, d.value / 10000)))
            .style("stroke", d => d.is_scam ? "rgba(255, 59, 92, 0.6)" : "rgba(154, 160, 166, 0.3)");
            
        linkElements = linkEnter.merge(linkElements);

        // Nodes
        nodeElements = nodeElements.data(d3Nodes, d => d.id);
        nodeElements.exit().remove();

        const nodeEnter = nodeElements.enter().append("circle")
            .attr("class", "node")
            .attr("r", d => Math.max(5, Math.min(30, 5 + d.totalRecv / 20000)))
            .style("fill", d => getNodeColor(d.type))
            .call(d3.drag()
                .on("start", dragStarted)
                .on("drag", dragged)
                .on("end", dragEnded)
            )
            .on("mouseover", (e, d) => {
                tooltip.transition().duration(200).style("opacity", .9);
                tooltip.html(`
                    <strong>Account:</strong> ${d.id}<br/>
                    <strong>Type:</strong> ${d.type}<br/>
                    <strong>Received:</strong> ₹${d.totalRecv.toLocaleString()}<br/>
                    <strong>Sent:</strong> ₹${d.totalSent.toLocaleString()}
                `)
                    .style("left", (e.pageX + 10) + "px")
                    .style("top", (e.pageY - 28) + "px");
                    
                // Highlight connected links
                linkElements.style('stroke', l => 
                    (l.source.id === d.id || l.target.id === d.id) ? '#fff' : 
                    (l.is_scam ? "rgba(255, 59, 92, 0.1)" : "rgba(154, 160, 166, 0.1)")
                ).style('stroke-opacity', l => (l.source.id === d.id || l.target.id === d.id) ? 1 : 0.2);
            })
            .on("mouseout", (e, d) => {
                tooltip.transition().duration(500).style("opacity", 0);
                linkElements.style('stroke', l => l.is_scam ? "rgba(255, 59, 92, 0.6)" : "rgba(154, 160, 166, 0.3)")
                            .style('stroke-opacity', 1);
            });

        nodeElements = nodeEnter.merge(nodeElements);

        // Update radius based on latest values
        nodeElements.transition().duration(300)
            .attr("r", d => Math.max(5, Math.min(30, 5 + d.totalRecv / 20000)))
            .style("fill", d => getNodeColor(d.type))
            // glow effect for victim/mule
            .style("filter", d => d.type === 'victim' || d.type === 'mule' ? `drop-shadow(0 0 5px ${getNodeColor(d.type)})` : 'none');

        // Restart simulation
        simulation.nodes(d3Nodes);
        simulation.force("link").links(d3Links);
        simulation.alpha(0.3).restart();
    }

    simulation.on("tick", () => {
        linkElements
            .attr("x1", d => d.source.x)
            .attr("y1", d => d.source.y)
            .attr("x2", d => d.target.x)
            .attr("y2", d => d.target.y);

        nodeElements
            .attr("cx", d => d.x)
            .attr("cy", d => d.y);
    });

    function getNodeColor(type) {
        if(type === 'victim') return '#ff3b5c';
        if(type === 'mule') return '#ffb83d';
        if(type === 'hub') return '#ff7043';
        return '#9aa0a6'; // normal
    }

    // Drag functions
    function dragStarted(e, d) {
        if (!e.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
    }
    function dragged(e, d) {
        d.fx = e.x; d.fy = e.y;
    }
    function dragEnded(e, d) {
        if (!e.active) simulation.alphaTarget(0);
        d.fx = null; d.fy = null;
    }

    // Listen for new transactions from feed
    window.addEventListener('kavach:new_txn', (e) => {
        const txn = e.detail;

        // Ensure nodes exist
        if (!nodesMap.has(txn.sender)) {
            nodesMap.set(txn.sender, { id: txn.sender, type: txn.is_scam ? 'victim' : 'normal', totalSent: 0, totalRecv: 0 });
        }
        if (!nodesMap.has(txn.receiver)) {
            // determine if mule (very basic heuristic for simulation)
            let type = 'normal';
            if (txn.is_scam) type = 'mule';
            nodesMap.set(txn.receiver, { id: txn.receiver, type: type, totalSent: 0, totalRecv: 0 });
        }

        const senderNode = nodesMap.get(txn.sender);
        const receiverNode = nodesMap.get(txn.receiver);

        senderNode.totalSent += txn.amount;
        receiverNode.totalRecv += txn.amount;

        // Escalate type if lots of rapid receipts
        if(receiverNode.totalRecv > 500000 && receiverNode.type === 'normal') {
            receiverNode.type = 'hub';
        }

        // Handle link
        const linkId = `${txn.sender}_${txn.receiver}`;
        if(linksMap.has(linkId)) {
            linksMap.get(linkId).value += txn.amount;
            if(txn.is_scam) linksMap.get(linkId).is_scam = true;
        } else {
            const link = { source: senderNode, target: receiverNode, value: txn.amount, is_scam: txn.is_scam };
            linksMap.set(linkId, link);
        }

        // Limit graph size roughly
        if(nodesMap.size > 80) {
            // Delete a random normal node that is not recently active
            let toDelete;
            for(let key of nodesMap.keys()){
                const n = nodesMap.get(key);
                if(n.type === 'normal') { toDelete = key; break; }
            }
            if(toDelete) {
                nodesMap.delete(toDelete);
                for(let lkey of linksMap.keys()) {
                    if(lkey.startsWith(toDelete+'_') || lkey.endsWith('_'+toDelete)) {
                        linksMap.delete(lkey);
                    }
                }
            }
        }

        // Sync to arrays
        d3Nodes = Array.from(nodesMap.values());
        d3Links = Array.from(linksMap.values());

        // We debounce the graph update slightly so it doesn't freeze on bursts
        if(window.graphUpdateTimer) clearTimeout(window.graphUpdateTimer);
        window.graphUpdateTimer = setTimeout(updateGraph, 50);
    });

    // Handle container resize
    window.addEventListener('resize', () => {
        width = container.clientWidth || 800;
        height = container.clientHeight || 500;
        simulation.force("center", d3.forceCenter(width / 2, height / 2))
                  .force('x', d3.forceX(width / 2).strength(0.05))
                  .force('y', d3.forceY(height / 2).strength(0.05));
        simulation.alpha(0.3).restart();
    });
});
