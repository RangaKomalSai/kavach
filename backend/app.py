import os
import json
import time
import uuid
import random
import threading
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import numpy as np

# RAG Imports (will be initialized on first use or startup)
try:
    from sentence_transformers import SentenceTransformer
    import faiss
    RAG_AVAILABLE = True
except ImportError:
    RAG_AVAILABLE = False
    print("WARNING: sentence-transformers or faiss not installed. RAG search will use mock mode.")

# =====================================================================
# CONFIGURATION
# =====================================================================
app = Flask(__name__)
CORS(app)  # Allow frontend to call across ports

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
FEED_FILE = os.path.join(DATA_DIR, "live_feed.json")
ALERTS_FILE = os.path.join(DATA_DIR, "alerts.json")
COMPLAINTS_FILE = os.path.join(DATA_DIR, "complaints_rag.csv")

MODE = os.environ.get("KAVACH_MODE", "DEMO") # "DEMO" or "LIVE"
MAX_FEED_LENGTH = 500

# =====================================================================
# RAG ENGINE (Lazy Initialization)
# =====================================================================
rag_model = None
rag_index = None
rag_df = None

def init_rag_engine():
    global rag_model, rag_index, rag_df
    if not RAG_AVAILABLE or rag_index is not None:
        return
    
    print("Initializing RAG Engine & FAISS Index...")
    try:
        if os.path.exists(COMPLAINTS_FILE):
            rag_df = pd.read_csv(COMPLAINTS_FILE)
            rag_model = SentenceTransformer('all-MiniLM-L6-v2')
            
            # Create embeddings for all narratives
            embeddings = rag_model.encode(rag_df['narrative'].tolist(), show_progress_bar=False)
            
            # Build FAISS index
            dimension = embeddings.shape[1]
            rag_index = faiss.IndexFlatL2(dimension)
            rag_index.add(np.array(embeddings).astype('float32'))
            print(f"FAISS index built successfully with {len(rag_df)} vectors.")
        else:
            print(f"Dataset not found at {COMPLAINTS_FILE}")
    except Exception as e:
        print(f"Failed to initialize RAG: {e}")

# =====================================================================
# BACKGROUND FEED GENERATOR (DEMO MODE)
# =====================================================================
# Ensure files exist empty initially
def ensure_json_file(filepath):
    if not os.path.exists(filepath):
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            json.dump([], f)

def read_json(filepath):
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def write_json(filepath, data):
    # Keep only the latest N to prevent infinite file size
    if filepath == FEED_FILE and len(data) > MAX_FEED_LENGTH:
        data = data[-MAX_FEED_LENGTH:]
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

def generate_normal_txn():
    names = ["rahul", "priya", "amit", "neha", "vikram", "anita", "suresh", "pooja"]
    banks = ["@ybl", "@sbi", "@okhdfc", "@okicici", "@pnb"]
    sender = random.choice(names) + str(random.randint(10, 99)) + random.choice(banks)
    receiver = random.choice(names) + str(random.randint(10, 99)) + random.choice(banks)
    
    return {
        "txn_id": f"TXN_{uuid.uuid4().hex[:8]}",
        "timestamp": datetime.now().isoformat() + "Z",
        "sender": sender,
        "receiver": receiver,
        "amount": round(random.uniform(50, 8000), 2),
        "is_flagged": False,
        "risk_score": round(random.uniform(0.01, 0.4), 2)
    }

def bg_feed_generator():
    if MODE != "DEMO":
        return
        
    print("Starting background transaction generator...")
    ensure_json_file(FEED_FILE)
    ensure_json_file(ALERTS_FILE)
    
    while True:
        try:
            feed = read_json(FEED_FILE)
            # Add 1-3 normal txns
            for _ in range(random.randint(1, 3)):
                feed.append(generate_normal_txn())
            write_json(FEED_FILE, feed)
        except Exception as e:
            print(f"BG Thread Error: {e}")
        time.sleep(random.uniform(1.0, 2.5))

# Start the background thread
bg_thread = threading.Thread(target=bg_feed_generator, daemon=True)
bg_thread.start()


# =====================================================================
# API ENDPOINTS
# =====================================================================

@app.route("/api/transactions", methods=["GET"])
def get_transactions():
    """Returns latest 50 transactions."""
    feed = read_json(FEED_FILE)
    return jsonify(feed[-50:])

@app.route("/api/simulate-scam", methods=["POST"])
def simulate_scam():
    """Injects a scam episode into the simulated feed."""
    feed = read_json(FEED_FILE)
    alerts = read_json(ALERTS_FILE)
    
    episode_id = f"EP_{uuid.uuid4().hex[:8]}"
    victim = f"victim_{random.randint(100,999)}@sbi"
    mules = [f"mule_{random.randint(10,99)}@ybl" for _ in range(3)]
    
    scam_txns = []
    total_amount = 0
    now = datetime.now()
    
    # Generate 6-10 fraudulent jumps
    num_txns = random.randint(6, 10)
    for i in range(num_txns):
        mule = random.choice(mules)
        amount = random.choice([25000, 50000, 100000, 200000])
        total_amount += amount
        
        # slight time offsets
        ts = pd.Timestamp(now) + pd.Timedelta(seconds=i*0.5)
        scam_txns.append({
            "txn_id": f"SCM_{uuid.uuid4().hex[:8]}",
            "timestamp": ts.isoformat() + "Z",
            "sender": victim,
            "receiver": mule,
            "amount": amount,
            "is_flagged": True,
            "risk_score": round(random.uniform(0.85, 0.99), 2),
            "episode_id": episode_id
        })
    
    feed.extend(scam_txns)
    write_json(FEED_FILE, feed)
    
    # Create Alert
    new_alert = {
        "alert_id": episode_id,
        "timestamp": now.isoformat() + "Z",
        "risk_score": 0.96,
        "accounts": [victim] + mules,
        "total_exposure": total_amount,
        "pattern_description": f"XGBoost FL_Model detected multiple high-value, uncharacteristic rounded transfers from a single sender to {len(mules)} young accounts.",
        "detection_time_seconds": round(random.uniform(0.5, 1.8), 2)
    }
    alerts.append(new_alert)
    write_json(ALERTS_FILE, alerts)
    
    return jsonify({"status": "scam_triggered", "episode_id": episode_id, "txns_injected": len(scam_txns)})

@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    """Returns active alerts list."""
    alerts = read_json(ALERTS_FILE)
    # Return latest first
    return jsonify(alerts[::-1])

@app.route("/api/network/<alert_id>", methods=["GET"])
def get_network(alert_id):
    """Returns a D3.js compatible network graph for an alert."""
    feed = read_json(FEED_FILE)
    
    # Filter txns that belong to this episode (or generic ones if not found)
    episode_txns = [t for t in feed if t.get("episode_id") == alert_id]
    
    nodes = {}
    edges = []
    
    for t in episode_txns:
        s, r = t["sender"], t["receiver"]
        
        if s not in nodes: nodes[s] = {"id": s, "type": "victim", "total_received": 0, "total_sent": 0}
        if r not in nodes: nodes[r] = {"id": r, "type": "mule", "total_received": 0, "total_sent": 0}
        
        nodes[s]["total_sent"] += t["amount"]
        nodes[r]["total_received"] += t["amount"]
        
        edges.append({
            "source": s,
            "target": r,
            "value": t["amount"],
            "is_scam": t.get("is_flagged", False)
        })
        
    return jsonify({
        "nodes": list(nodes.values()),
        "edges": edges
    })

@app.route("/api/rag/search", methods=["POST"])
def rag_search():
    """Semantic search over past complaints using FAISS and MiniLM."""
    data = request.json
    query = data.get("query", "")
    if not query:
        return jsonify({"error": "No query provided"}), 400
        
    init_rag_engine() # Lazy load
    
    if rag_index is None:
        # Fallback Mock Mode
        return jsonify({
            "results": [
                {
                    "complaint_id": "MOCK_01",
                    "narrative": "Search index not loaded. Returning mock result for: " + query[:20],
                    "similarity": 0.85,
                    "amount_lost": 50000,
                    "city": "Mumbai"
                }
            ]
        })
        
    # Real Vector Search
    query_vector = rag_model.encode([query])
    distances, indices = rag_index.search(np.array(query_vector).astype('float32'), k=5)
    
    results = []
    for i, idx in enumerate(indices[0]):
        if idx == -1: continue # fewer results than k
        row = rag_df.iloc[idx]
        
        # Convert distance to a pseudo-similarity score (0 to 1)
        sim = 1 / (1 + float(distances[0][i]))
        
        results.append({
            "complaint_id": str(row["complaint_id"]),
            "narrative": str(row["narrative"])[:300] + "...", # trunk to 300 chars for API
            "similarity": round(sim, 3),
            "amount_lost": float(row["amount_lost"]),
            "city": str(row["victim_city"])
        })
        
    return jsonify({"results": results})

@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    """Returns static model performance metrics."""
    return jsonify({
        "f1": 0.932,
        "auc": 0.968,
        "precision": 0.924,
        "recall": 0.941,
        "avg_detection_time": 1.2
    })


if __name__ == "__main__":
    ensure_json_file(FEED_FILE)
    ensure_json_file(ALERTS_FILE)
    print("Starting KAVACH Backend in DEMO Mode...")
    app.run(host="0.0.0.0", port=8080, debug=False)
