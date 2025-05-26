#!/usr/bin/env python3
"""
Flask-based patch utility to fix missing data in Postgres from pybaseball
Browser-based progress monitoring and control
"""
import os
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, render_template_string, jsonify, request
import pandas as pd
from pybaseball import statcast
from sqlalchemy import create_engine, text
import logging

# Global status tracking
patch_status = {
    "status": "Idle",
    "rows_scanned": 0,
    "rows_updated": 0,
    "current_processing": "",
    "start_time": None,
    "elapsed_time": 0,
    "error_message": "",
    "total_expected": 0
}

app = Flask(__name__)
logger = logging.getLogger(__name__)

# HTML Template for the monitor
MONITOR_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>SwordFinder Database Patch Monitor</title>
    <meta http-equiv="refresh" content="3">
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #1a1a1a; color: #fff; }
        .container { max-width: 800px; margin: 0 auto; }
        .status-card { background: #2d2d2d; padding: 20px; border-radius: 10px; margin: 20px 0; }
        .status-running { border-left: 5px solid #4CAF50; }
        .status-idle { border-left: 5px solid #FFC107; }
        .status-error { border-left: 5px solid #F44336; }
        .status-completed { border-left: 5px solid #2196F3; }
        .progress-bar { background: #444; height: 20px; border-radius: 10px; overflow: hidden; }
        .progress-fill { background: #4CAF50; height: 100%; transition: width 0.3s; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .stat { background: #333; padding: 15px; border-radius: 8px; text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #4CAF50; }
        .button { background: #4CAF50; color: white; padding: 12px 24px; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }
        .button:disabled { background: #666; cursor: not-allowed; }
        .log { background: #1e1e1e; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; max-height: 300px; overflow-y: auto; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🗡️ SwordFinder Database Patch Monitor</h1>
        
        <div class="status-card status-{{ status.status.lower() }}">
            <h2>Status: {{ status.status }}</h2>
            <p><strong>Current Task:</strong> {{ status.current_processing or "Waiting for commands" }}</p>
            <p><strong>Elapsed Time:</strong> {{ status.elapsed_time }}s</p>
            
            {% if status.total_expected > 0 %}
            <div class="progress-bar">
                <div class="progress-fill" style="width: {{ (status.rows_scanned / status.total_expected * 100) if status.total_expected > 0 else 0 }}%"></div>
            </div>
            <p>{{ status.rows_scanned }} / {{ status.total_expected }} rows processed</p>
            {% endif %}
        </div>

        <div class="stats">
            <div class="stat">
                <div class="stat-value">{{ status.rows_scanned }}</div>
                <div>Rows Scanned</div>
            </div>
            <div class="stat">
                <div class="stat-value">{{ status.rows_updated }}</div>
                <div>Rows Updated</div>
            </div>
            <div class="stat">
                <div class="stat-value">{{ "%.1f"|format(status.rows_updated / status.rows_scanned * 100) if status.rows_scanned > 0 else 0 }}%</div>
                <div>Update Rate</div>
            </div>
        </div>

        <div style="margin: 30px 0;">
            <form action="/start" method="post" style="display: inline;">
                <button type="submit" class="button" {{ "disabled" if status.status == "Running" else "" }}>
                    Start Database Patch
                </button>
            </form>
        </div>

        {% if status.error_message %}
        <div class="status-card status-error">
            <h3>Error Details</h3>
            <div class="log">{{ status.error_message }}</div>
        </div>
        {% endif %}

        <div class="status-card">
            <h3>Process Log</h3>
            <div class="log" id="log">
                <div>{{ now }} - System ready for database patching</div>
                {% if status.status != "Idle" %}
                <div>{{ status.start_time }} - Patch process started</div>
                {% endif %}
            </div>
        </div>
    </div>
</body>
</html>
"""

@app.route('/monitor')
def monitor():
    """Main monitoring dashboard"""
    global patch_status
    
    # Calculate elapsed time
    if patch_status["start_time"]:
        patch_status["elapsed_time"] = int(time.time() - patch_status["start_time"])
    
    return render_template_string(MONITOR_HTML, 
                                status=patch_status, 
                                now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

@app.route('/status.json')
def status_json():
    """JSON endpoint for status polling"""
    return jsonify(patch_status)

@app.route('/start', methods=['POST'])
def start_patch():
    """Start the patching process"""
    global patch_status
    
    if patch_status["status"] == "Running":
        return "Already running", 400
    
    # Reset status
    patch_status.update({
        "status": "Running",
        "rows_scanned": 0,
        "rows_updated": 0,
        "current_processing": "Initializing...",
        "start_time": time.time(),
        "error_message": ""
    })
    
    # Start patch in background thread
    thread = threading.Thread(target=run_patch_process)
    thread.daemon = True
    thread.start()
    
    return "Patch started", 200

def run_patch_process():
    """Main patching process - runs in background"""
    global patch_status
    
    try:
        database_url = os.environ.get('DATABASE_URL')
        engine = create_engine(database_url)
        
        # Define date range to patch (recent dates first)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)  # Last 7 days
        
        patch_status["current_processing"] = f"Pulling Statcast data from {start_date} to {end_date}"
        
        # Pull fresh data from pybaseball
        df = statcast(start_dt=str(start_date), end_dt=str(end_date))
        patch_status["total_expected"] = len(df)
        
        with engine.connect() as conn:
            batch_size = 500
            updated_count = 0
            
            for i in range(0, len(df), batch_size):
                if patch_status["status"] != "Running":  # Allow stopping
                    break
                    
                batch = df.iloc[i:i+batch_size]
                patch_status["current_processing"] = f"Processing batch {i//batch_size + 1} (rows {i}-{min(i+batch_size, len(df))})"
                
                for idx, row in batch.iterrows():
                    patch_status["rows_scanned"] = idx + 1
                    
                    # Build update query for missing fields
                    update_fields = []
                    params = {
                        'game_pk': int(row['game_pk']) if pd.notna(row['game_pk']) else None,
                        'at_bat_number': int(row['at_bat_number']) if pd.notna(row['at_bat_number']) else None,
                        'pitch_number': int(row['pitch_number']) if pd.notna(row['pitch_number']) else None
                    }
                    
                    # Add fields that might be missing
                    if pd.notna(row['release_speed']):
                        update_fields.append("release_speed = COALESCE(release_speed, :release_speed)")
                        params['release_speed'] = float(row['release_speed'])
                    
                    if pd.notna(row['home_team']):
                        update_fields.append("home_team = COALESCE(home_team, :home_team)")
                        params['home_team'] = str(row['home_team'])
                    
                    if pd.notna(row['away_team']):
                        update_fields.append("away_team = COALESCE(away_team, :away_team)")
                        params['away_team'] = str(row['away_team'])
                    
                    if pd.notna(row['release_spin_rate']):
                        update_fields.append("release_spin_rate = COALESCE(release_spin_rate, :release_spin_rate)")
                        params['release_spin_rate'] = float(row['release_spin_rate'])
                    
                    if pd.notna(row['plate_x']):
                        update_fields.append("plate_x = COALESCE(plate_x, :plate_x)")
                        params['plate_x'] = float(row['plate_x'])
                    
                    if pd.notna(row['plate_z']):
                        update_fields.append("plate_z = COALESCE(plate_z, :plate_z)")
                        params['plate_z'] = float(row['plate_z'])
                    
                    if pd.notna(row['pitch_name']):
                        update_fields.append("pitch_name = COALESCE(pitch_name, :pitch_name)")
                        params['pitch_name'] = str(row['pitch_name'])
                    
                    # Only update if we have fields and valid keys
                    if update_fields and all(params[k] is not None for k in ['game_pk', 'at_bat_number', 'pitch_number']):
                        update_query = text(f"""
                            UPDATE statcast_pitches 
                            SET {', '.join(update_fields)}
                            WHERE game_pk = :game_pk
                            AND at_bat_number = :at_bat_number
                            AND pitch_number = :pitch_number
                        """)
                        
                        result = conn.execute(update_query, params)
                        if result.rowcount > 0:
                            updated_count += result.rowcount
                            patch_status["rows_updated"] = updated_count
                
                # Commit after each batch
                conn.commit()
        
        patch_status["status"] = "Completed"
        patch_status["current_processing"] = f"Patch completed! Updated {updated_count} records"
        
    except Exception as e:
        patch_status["status"] = "Error"
        patch_status["error_message"] = str(e)
        patch_status["current_processing"] = "Error occurred during patching"

@app.route('/')
def index():
    """Redirect to monitor"""
    return """
    <h1>🗡️ SwordFinder Database Patch Control</h1>
    <p><a href="/monitor">Open Patch Monitor →</a></p>
    """

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)