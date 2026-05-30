let token = "";
let pollInterval;
let globalNodes = [];
let globalTasks = [];
let network;

let nodesData = new vis.DataSet([]);
let edgesData = new vis.DataSet([]);

function initGraph() {
    const container = document.getElementById('network-container');
    const data = { nodes: nodesData, edges: edgesData };
    
    const options = {
        nodes: { shape: 'dot', font: { color: '#ffffff' }, borderWidth: 2 },
        edges: { width: 2, color: { color: '#555', highlight: '#007acc' }, smooth: { type: 'continuous' } },
        physics: { barnesHut: { gravitationalConstant: -3000, centralGravity: 0.3, springLength: 150 } }
    };
    
    network = new vis.Network(container, data, options);

    network.on("click", function (params) {
        if (params.nodes.length > 0) {
            showDetails(params.nodes[0]);
        } else {
            document.getElementById('details').innerHTML = "";
        }
    });

    nodesData.update({ id: 'master', label: 'MASTER\n(Controller)', size: 30, color: { background: '#007acc', border: '#005a9e' } });
}

function startConnection() {
    token = document.getElementById('tokenInput').value;
    if (!token) { alert("Please enter the token!"); return; }
    
    document.getElementById('status-msg').innerText = "Connected. Polling live data...";
    fetchData(); 
    
    if(pollInterval) clearInterval(pollInterval);
    pollInterval = setInterval(fetchData, 2000); 
}

async function fetchData() {
    try {
        const headers = { 'Authorization': 'Bearer ' + token };
        const [nodesRes, tasksRes] = await Promise.all([
            fetch('/nodes', { headers }),
            fetch('/tasks', { headers })
        ]);
        
        if (!nodesRes.ok) throw new Error("Auth failed!");
        
        globalNodes = await nodesRes.json() || [];
        globalTasks = await tasksRes.json() || [];
        
        updateGraph();
        
        let selectedNodes = network.getSelectedNodes();
        if (selectedNodes.length > 0) showDetails(selectedNodes[0]);

    } catch (e) {
        console.error(e);
        document.getElementById('status-msg').innerHTML = "<span class='text-error'>Connection lost or invalid token!</span>";
        clearInterval(pollInterval);
    }
}

function updateGraph() {
    let activeWorkerIds = new Set();

    globalNodes.forEach(node => {
        activeWorkerIds.add(node.id);
        let isDown = node.state === "DOWN";
        
        let bgColor = isDown ? '#f44336' : '#4caf50';
        let borderColor = isDown ? '#b71c1c' : '#2e7d32';
        let labelText = `Worker\n${node.ip}`;

        nodesData.update({ id: node.id, label: labelText, size: 20, color: { background: bgColor, border: borderColor }});
        edgesData.update({ id: `edge-${node.id}`, from: 'master', to: node.id, dashes: isDown });
    });
}

function showDetails(nodeId) {
    let html = "";
    if (nodeId === 'master') {
        let pendingTasks = globalTasks.filter(t => t.State === "PENDING");
        html = `<div class="detail-box">
                    <h4>Master Node</h4>
                    <p>Status: <span class="badge">ACTIVE</span></p>
                    <p>Total Workers: <b>${globalNodes.length}</b></p>
                    <p>Total Tasks: <b>${globalTasks.length}</b></p>
                    <p>Pending Tasks (Waiting): <b>${pendingTasks.length}</b></p>
                </div>`;
    } else {
        let node = globalNodes.find(n => n.id === nodeId);
        if (!node) return;

        let nodeTasks = globalTasks.filter(t => t.WorkerID === node.id && t.State === "RUNNING");
        let badgeClass = node.state === "DOWN" ? "badge down" : "badge";
        
        html = `<div class="detail-box">
                    <h4>Worker Node</h4>
                    <p>ID: <small>${node.id}</small></p>
                    <p>IP: <b>${node.ip}</b></p>
                    <p>API Port: <b>${node.api_port}</b></p>
                    <p>Status: <span class="${badgeClass}">${node.state}</span></p>
                    <p>Free RAM: <b>${node.memory_free} MB</b></p>
                </div>
                <h4 class="section-title">Running Tasks (${nodeTasks.length}):</h4>`;
        
        if (nodeTasks.length === 0) {
            html += `<p class="text-muted">No tasks running on this node.</p>`;
        } else {
            nodeTasks.forEach(t => {
                html += `<div class="task-item">
                            <b>Image: ${t.Image}</b><br>
                            <span class="text-muted">ID:</span> ${t.ID.substring(0,8)}<br>
                            <span class="text-muted">Container:</span> ${t.ContainerName}
                         </div>`;
            });
        }
    }
    document.getElementById('details').innerHTML = html;
}

window.onload = initGraph;