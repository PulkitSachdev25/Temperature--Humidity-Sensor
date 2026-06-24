import threading
import queue
from datetime import datetime
import socket
import urllib.parse
import winsound
import os
import json
import webbrowser
import time
import mysql.connector  # Runs the connection to your online cloud database
from dotenv import load_dotenv

load_dotenv()
# 1. Thread-safe history list and queue
data_queue = queue.Queue()
data_history = []
history_lock = threading.Lock()

# Memory tracking to combine alternating packets and track changes accurately
latest_state = {"temp": None, "humidity": None}
last_logged_state = {"temp": None, "humidity": None}

# =====================================================================
# STEP 1: PASTE YOUR ONLINE CLOUD DATABASE CREDENTIALS HERE!
# =====================================================================
DB_CONFIG = {
    'host': os.environ.get('Host'),        # 👈 Added quotes here to remove the yellow line!
    'port': int(os.environ.get('Port', 27133)), 
    'user': os.environ.get('User', 'avnadmin'),
    'password': os.environ.get('Password'),
    'database': os.environ.get('Database', 'defaultdb'),
    'ssl_verify_cert': False,
    'ssl_ca': None
}

def extract_sensor_data(payload):
    """Extracts data. Returns None if the key is missing in this specific packet."""
    parsed = urllib.parse.parse_qs(payload)
    temp = parsed.get('T0', [None])[0]
    humidity = parsed.get('H0', [None])[0]
    return temp, humidity

def init_db():
    """Initializes the Cloud MySQL database and preloads the last 100 rows into memory."""
    global data_history
    try:
        print("Connecting to Cloud MySQL database...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist on the cloud yet
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetry_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp VARCHAR(20) NOT NULL,
                temperature VARCHAR(10) DEFAULT NULL,
                humidity VARCHAR(10) DEFAULT NULL,
                raw_payload TEXT
            )
        """)
        conn.commit()
        
        # Preload the last 100 entries so your dashboard opens up fully populated
        cursor.execute("SELECT timestamp, temperature, humidity, raw_payload FROM telemetry_logs ORDER BY id DESC LIMIT 100")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Reverse the order so the oldest database entry is index 0 for the frontend chart timeline
        with history_lock:
            for row in reversed(rows):
                data_history.append({
                    "time": row[0],
                    "temp": str(row[1]),
                    "humidity": str(row[2]),
                    "payload": row[3]
                })
        print(f"Cloud MySQL Database ready. Preloaded {len(rows)} records successfully.")
    except Exception as e:
        print(f"⚠️ Cloud database initialization failed: {e}")
        print("The server will still run, but logs will not be saved to the cloud.")


# 2. High-Speed Direct HTTP Handling Server & Web Interface
def run_http_socket_server():
    HOST = '0.0.0.0'
    PORT = 5000
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(15) 
    
    print(f"High-Speed HTTP Interceptor & Dashboard Server listening on port {PORT}...")
    
    while True:
        client_sock = None
        try:
            client_sock, client_addr = server.accept()
            client_sock.settimeout(2.0) 
            
            try:
                request_bytes = client_sock.recv(4096)
            except socket.timeout:
                request_bytes = b""
                
            if request_bytes:
                raw_request = request_bytes.decode('utf-8', errors='ignore')
                
                # --- ROUTE 1: INCOMING LOGGER DATA ---
                if "POST " in raw_request and "IMEI=" in raw_request:
                    raw_data = raw_request.split("\r\n\r\n")[-1]
                    if not raw_data:
                        for line in raw_request.split("\r\n"):
                            if line.startswith("IMEI="):
                                raw_data = line
                                break
                    
                    if "IMEI=" in raw_data:
                        current_time = datetime.now().strftime("%H:%M:%S")
                        temp, humidity = extract_sensor_data(raw_data)
                        
                        # Fixes the stuck value loops by evaluating value updates independently
                        has_changed = False

                        if temp is not None and temp != last_logged_state["temp"]:
                            latest_state["temp"] = temp
                            last_logged_state["temp"] = temp
                            has_changed = True

                        if humidity is not None and humidity != last_logged_state["humidity"]:
                            latest_state["humidity"] = humidity
                            last_logged_state["humidity"] = humidity
                            has_changed = True
                        
                        # Trigger data logging only if we have full metrics AND an active change has been registered
                        if has_changed and latest_state["temp"] is not None and latest_state["humidity"] is not None:
                            
                            record = {
                                "time": current_time,
                                "temp": latest_state["temp"],
                                "humidity": latest_state["humidity"],
                                "payload": raw_data
                            }
                            
                            with history_lock:
                                data_history.append(record)
                                if len(data_history) > 1000:
                                    data_history.pop(0)
                                    
                                # CLOUD MYSQL STORAGE: Push data row up to the cloud database live
                                try:
                                    conn = mysql.connector.connect(**DB_CONFIG)
                                    cursor = conn.cursor()
                                    cursor.execute(
                                        "INSERT INTO telemetry_logs (timestamp, temperature, humidity, raw_payload) VALUES (%s, %s, %s, %s)",
                                        (current_time, str(latest_state["temp"]), str(latest_state["humidity"]), raw_data)
                                    )
                                    conn.commit()
                                    cursor.close()
                                    conn.close()
                                except Exception as db_err:
                                    print(f"Cloud Database write error: {db_err}")
                            
                            print(f"[{current_time}] Cloud Logged: Temp={latest_state['temp']}°C, Hum={latest_state['humidity']}%")
                            
                            # High-temperature warning buzzer
                            try:
                                if float(latest_state["temp"]) >= 30.0:
                                    threading.Thread(target=lambda: winsound.Beep(1000, 400), daemon=True).start()
                            except ValueError:
                                pass
                    
                    response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nOK"
                    client_sock.sendall(response.encode('utf-8'))
                
                # --- ROUTE 2: API DATA REQUEST ---
                elif "GET /api/data" in raw_request:
                    with history_lock:
                        json_data = json.dumps(data_history)
                    response = (
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: application/json\r\n"
                        f"Content-Length: {len(json_data.encode('utf-8'))}\r\n"
                        "Connection: close\r\n\r\n"
                        f"{json_data}"
                    )
                    client_sock.sendall(response.encode('utf-8'))
                    
                # --- ROUTE 3: SERVE HTML DASHBOARD ---
                elif "GET /" in raw_request:
                    base_dir = os.path.dirname(os.path.abspath(__file__))
                    dashboard_path = os.path.join(base_dir, "dashboard.html")
                    try:
                        with open(dashboard_path, "r", encoding="utf-8") as f:
                            html_content = f.read()
                        response = (
                            "HTTP/1.1 200 OK\r\n"
                            "Content-Type: text/html; charset=utf-8\r\n"
                            f"Content-Length: {len(html_content.encode('utf-8'))}\r\n"
                            "Connection: close\r\n\r\n"
                            f"{html_content}"
                        )
                    except Exception as e:
                        err_msg = f"Error reading dashboard.html: {str(e)}"
                        response = f"HTTP/1.1 500 Error\r\nContent-Length: {len(err_msg)}\r\nConnection: close\r\n\r\n{err_msg}"
                    client_sock.sendall(response.encode('utf-8'))
                    
                else:
                    response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    client_sock.sendall(response.encode('utf-8'))
            
        except Exception:
            pass
        finally:
            if client_sock:
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                client_sock.close()

if __name__ == '__main__':
    # Make sure your credentials inside DB_CONFIG match your Aiven info!
    init_db()
    
    server_thread = threading.Thread(target=run_http_socket_server, daemon=True)
    server_thread.start()
    
    time.sleep(0.5)
    dashboard_url = "http://localhost:5000/"
    print(f"Launching dashboard: {dashboard_url}")
    webbrowser.open(dashboard_url)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down server.")