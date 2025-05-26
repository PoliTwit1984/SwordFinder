"""
CSV Database Patch with Real-Time Web Monitoring
Updates database from CSV with live progress tracking
"""
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime
import logging
import threading
from flask import Flask, render_template_string, jsonify

# Global status tracking
patch_status = {
    "status": "Ready",
    "total_records": 0,
    "processed": 0,
    "updated": 0,
    "current_batch": 0,
    "start_time": None,
    "elapsed_time": 0,
    "error_message": ""
}

app = Flask(__name__)

@app.route('/csv-patch-monitor')
def monitor():
    """Real-time CSV patch monitoring interface"""
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>🗡️ CSV Database Patch Monitor</title>
        <meta http-equiv="refresh" content="2">
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #1a1a1a; color: #fff; }
            .container { max-width: 900px; margin: 0 auto; }
            .status-card { background: #2d2d2d; padding: 25px; border-radius: 12px; margin: 20px 0; }
            .status-running { border-left: 6px solid #4CAF50; }
            .status-ready { border-left: 6px solid #FFC107; }
            .status-error { border-left: 6px solid #F44336; }
            .status-completed { border-left: 6px solid #2196F3; }
            .progress-bar { background: #444; height: 25px; border-radius: 12px; overflow: hidden; margin: 15px 0; }
            .progress-fill { background: linear-gradient(90deg, #4CAF50, #45a049); height: 100%; transition: width 0.5s; }
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 18px; margin: 25px 0; }
            .stat { background: #333; padding: 20px; border-radius: 10px; text-align: center; }
            .stat-value { font-size: 2.2em; font-weight: bold; color: #4CAF50; margin-bottom: 8px; }
            .stat-label { color: #bbb; font-size: 0.9em; }
            .button { background: #4CAF50; color: white; padding: 15px 30px; border: none; border-radius: 8px; cursor: pointer; font-size: 16px; margin: 10px; }
            .button:disabled { background: #666; cursor: not-allowed; }
            .highlight { color: #4CAF50; font-weight: bold; }
            h1 { text-align: center; color: #4CAF50; margin-bottom: 30px; }
            .eta { background: #444; padding: 15px; border-radius: 8px; margin: 15px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🗡️ CSV Database Patch Monitor</h1>
            
            <div class="status-card status-{{ status.status.lower().replace(' ', '-') }}">
                <h2>Status: {{ status.status }}</h2>
                {% if status.status == "Running" %}
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {{ (status.processed / status.total_records * 100) if status.total_records > 0 else 0 }}%"></div>
                    </div>
                    <p><strong>Progress:</strong> {{ status.processed }} / {{ status.total_records }} records processed</p>
                    <p><strong>Current Batch:</strong> {{ status.current_batch }}</p>
                    {% if status.processed > 0 and status.total_records > 0 %}
                        <div class="eta">
                            <strong>ETA:</strong> 
                            {% set rate = status.processed / status.elapsed_time if status.elapsed_time > 0 else 0 %}
                            {% set remaining = status.total_records - status.processed %}
                            {% set eta_seconds = (remaining / rate) if rate > 0 else 0 %}
                            {{ "%.1f"|format(eta_seconds / 60) }} minutes remaining
                        </div>
                    {% endif %}
                {% endif %}
                <p><strong>Elapsed Time:</strong> {{ "%.1f"|format(status.elapsed_time) }}s</p>
                {% if status.error_message %}
                    <p style="color: #F44336;"><strong>Error:</strong> {{ status.error_message }}</p>
                {% endif %}
            </div>
            
            <div class="stats">
                <div class="stat">
                    <div class="stat-value">{{ "{:,}"|format(status.total_records) }}</div>
                    <div class="stat-label">Total Records</div>
                </div>
                <div class="stat">
                    <div class="stat-value">{{ "{:,}"|format(status.processed) }}</div>
                    <div class="stat-label">Processed</div>
                </div>
                <div class="stat">
                    <div class="stat-value">{{ "{:,}"|format(status.updated) }}</div>
                    <div class="stat-label">Updated</div>
                </div>
                <div class="stat">
                    <div class="stat-value">{{ "%.1f"|format((status.processed / status.elapsed_time) if status.elapsed_time > 0 else 0) }}</div>
                    <div class="stat-label">Records/sec</div>
                </div>
            </div>
            
            <div style="text-align: center; margin-top: 30px;">
                <button class="button" onclick="startPatch()" 
                        {% if status.status == "Running" %}disabled{% endif %}>
                    Start CSV Patch
                </button>
            </div>
        </div>
        
        <script>
            function startPatch() {
                fetch('/start-csv-patch', {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        location.reload();
                    } else {
                        alert('Error: ' + data.error);
                    }
                });
            }
        </script>
    </body>
    </html>
    """, status=patch_status)

@app.route('/start-csv-patch', methods=['POST'])
def start_csv_patch():
    """Start the CSV patch process"""
    global patch_status
    
    if patch_status["status"] == "Running":
        return jsonify({"success": False, "error": "Patch already running"})
    
    # Reset status
    patch_status.update({
        "status": "Running",
        "total_records": 0,
        "processed": 0,
        "updated": 0,
        "current_batch": 0,
        "start_time": time.time(),
        "elapsed_time": 0,
        "error_message": ""
    })
    
    # Start patch in background
    thread = threading.Thread(target=run_csv_patch)
    thread.daemon = True
    thread.start()
    
    return jsonify({"success": True})

def safe_float(value):
    """Safely convert to float"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return float(value)
    except:
        return None

def safe_int(value):
    """Safely convert to int"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return int(value)
    except:
        return None

def safe_str(value):
    """Safely convert to string"""
    if pd.isna(value) or value == '' or value is None:
        return None
    return str(value)

def run_csv_patch():
    """Main CSV patch process with monitoring"""
    global patch_status
    
    try:
        # Connect to database
        database_url = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Load CSV
        df = pd.read_csv('complete_statcast_2025.csv', low_memory=False)
        patch_status["total_records"] = len(df)
        
        # Prepare update query
        update_query = """
        UPDATE statcast_pitches 
        SET 
            home_team = COALESCE(home_team, %s),
            away_team = COALESCE(away_team, %s),
            release_speed = COALESCE(release_speed, %s),
            release_spin_rate = COALESCE(release_spin_rate, %s),
            spin_axis = COALESCE(spin_axis, %s),
            plate_x = COALESCE(plate_x, %s),
            plate_z = COALESCE(plate_z, %s),
            pitch_name = COALESCE(pitch_name, %s),
            stand = COALESCE(stand, %s),
            p_throws = COALESCE(p_throws, %s),
            sz_top = COALESCE(sz_top, %s),
            sz_bot = COALESCE(sz_bot, %s),
            pfx_x = COALESCE(pfx_x, %s),
            pfx_z = COALESCE(pfx_z, %s),
            effective_speed = COALESCE(effective_speed, %s),
            release_extension = COALESCE(release_extension, %s)
        WHERE game_pk = %s 
        AND game_date = %s
        AND pitcher = %s
        AND batter = %s
        """
        
        batch_size = 1000
        
        for i in range(0, len(df), batch_size):
            if patch_status["status"] != "Running":
                break
                
            batch = df.iloc[i:i+batch_size]
            batch_data = []
            
            for _, row in batch.iterrows():
                # Skip if missing critical matching fields
                if pd.isna(row.get('game_pk')) or pd.isna(row.get('game_date')):
                    continue
                    
                update_data = (
                    safe_str(row.get('home_team')),
                    safe_str(row.get('away_team')),
                    safe_float(row.get('release_speed')),
                    safe_float(row.get('release_spin_rate')),
                    safe_float(row.get('spin_axis')),
                    safe_float(row.get('plate_x')),
                    safe_float(row.get('plate_z')),
                    safe_str(row.get('pitch_name')),
                    safe_str(row.get('stand')),
                    safe_str(row.get('p_throws')),
                    safe_float(row.get('sz_top')),
                    safe_float(row.get('sz_bot')),
                    safe_float(row.get('pfx_x')),
                    safe_float(row.get('pfx_z')),
                    safe_float(row.get('effective_speed')),
                    safe_float(row.get('release_extension')),
                    # WHERE conditions
                    safe_int(row.get('game_pk')),
                    safe_str(row.get('game_date')),
                    safe_int(row.get('pitcher')),
                    safe_int(row.get('batter'))
                )
                batch_data.append(update_data)
            
            # Execute batch
            if batch_data:
                cursor.executemany(update_query, batch_data)
                patch_status["updated"] += cursor.rowcount
                conn.commit()
            
            # Update status
            patch_status["processed"] = min(i + batch_size, len(df))
            patch_status["current_batch"] = i // batch_size + 1
            patch_status["elapsed_time"] = time.time() - patch_status["start_time"]
        
        cursor.close()
        conn.close()
        patch_status["status"] = "Completed"
        
    except Exception as e:
        patch_status["status"] = "Error"
        patch_status["error_message"] = str(e)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)