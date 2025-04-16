#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import threading
import time
from flask import Flask, render_template_string, request # request needed for shutdown

# --- Configuration ---
BROKER_HOST = "localhost"     # Runs on same server as broker usually
BROKER_PORT = 1883
MQTT_USERNAME = "iotuser"     # User for the listener
MQTT_PASSWORD = "iot"         # Password for the listener

# --- Trigger Configuration ---
TRIGGER_TOPIC = "prison/security/lightsout/" # The EXACT topic to listen on
TRIGGER_PAYLOAD = "getout"                 # The EXACT payload to trigger success

# --- CTF Flag ---
FINAL_FLAG = "FLAG{LIGHTS_OUT_AND_IM_GONE}" # Your creative final flag

# --- Web App Configuration ---
WEB_HOST = '0.0.0.0' # Listen on all interfaces
WEB_PORT = 5000      # Port for the hidden website

# --- Global Status ---
# Possible states: "RED", "GREEN"
security_state = "RED"
state_lock = threading.Lock()

# --- MQTT Listener Logic for Web Server ---
mqtt_client = None
mqtt_connected = False
exit_app = threading.Event() # To signal MQTT thread to stop

def on_web_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        print("[WEB-MQTT] Listener connected successfully.")
        mqtt_connected = True
        # Subscribe ONLY to the trigger topic
        client.subscribe(TRIGGER_TOPIC, qos=1)
    else:
        print(f"[WEB-MQTT] Listener connection failed code {rc}")
        mqtt_connected = False

def on_web_subscribe(client, userdata, mid, granted_qos):
     print(f"[WEB-MQTT] Subscribed to trigger topic: {TRIGGER_TOPIC}")

def on_web_message(client, userdata, msg):
    global security_state
    topic = msg.topic
    try:
        payload = msg.payload.decode("utf-8")
        print(f"[WEB-MQTT] Received on '{topic}': {payload}") # Log received messages

        # Check if it's the exact trigger topic and payload
        if topic == TRIGGER_TOPIC and payload == TRIGGER_PAYLOAD:
            print(f"[WEB-MQTT] Correct trigger received! Updating status to GREEN and revealing flag.")
            with state_lock:
                # Only change state if it's currently RED
                if security_state == "RED":
                    security_state = "GREEN"
            # No need to publish anything back from here
        else:
            print(f"[WEB-MQTT] Ignoring message (doesn't match trigger).")

    except Exception as e:
        print(f"[WEB-MQTT] Error processing message: {e}")

def on_web_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    if rc != 0 and not exit_app.is_set():
        print(f"[WEB-MQTT] Listener unexpectedly disconnected (rc={rc}). Will retry connection...")
        # Reconnect logic is handled by the loop in mqtt_listener_thread

def mqtt_listener_thread():
    global mqtt_client, mqtt_connected
    print("[WEB-MQTT] Starting MQTT listener thread...")
    # Use the correct callback API version
    try:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="hidden-website-status-listener")
    except AttributeError:
        print("[WEB-MQTT] Using older MQTT Callback API Version 1.")
        mqtt_client = mqtt.Client(client_id="hidden-website-status-listener")

    mqtt_client.on_connect = on_web_connect
    mqtt_client.on_subscribe = on_web_subscribe
    mqtt_client.on_message = on_web_message
    mqtt_client.on_disconnect = on_web_disconnect

    if MQTT_USERNAME and MQTT_PASSWORD:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    while not exit_app.is_set():
         if not mqtt_connected:
              try:
                   print(f"[WEB-MQTT] Attempting connection to {BROKER_HOST}...")
                   mqtt_client.connect(BROKER_HOST, BROKER_PORT, 60)
                   mqtt_connected = True # Tentatively set true
                   # loop_forever() is blocking and handles reconnects after initial success
                   mqtt_client.loop_forever()
                   # If loop_forever exits, it means disconnect happened.
                   print("[WEB-MQTT] loop_forever exited. Will attempt reconnect cycle.")
                   mqtt_connected = False # Ensure flag is reset if loop exits

              except TimeoutError:
                   print("[WEB-MQTT] Connection attempt timed out. Retrying in 10s...")
                   mqtt_connected = False; time.sleep(10)
              except ConnectionRefusedError:
                   print("[WEB-MQTT] Connection refused. Retrying in 10s...")
                   mqtt_connected = False; time.sleep(10)
              except OSError as e:
                   print(f"[WEB-MQTT] Network error during connect/loop: {e}. Retrying in 10s...")
                   mqtt_connected = False; time.sleep(10)
              except Exception as e:
                   print(f"[WEB-MQTT] Unexpected MQTT error: {e}. Retrying in 10s...")
                   mqtt_connected = False; time.sleep(10)
         else:
             # This part should theoretically not be reached if loop_forever is working
             print("[WEB-MQTT] In unexpected state (connected=True but loop not running?). Resetting.")
             mqtt_connected = False
             time.sleep(5) # Brief pause before retry cycle

    print("[WEB-MQTT] Listener thread received exit signal.")
    if mqtt_client and mqtt_client.is_connected():
        try:
            # loop_stop needs to be called *before* disconnect sometimes
            print("[WEB-MQTT] Stopping MQTT loop...")
            mqtt_client.loop_stop()
            print("[WEB-MQTT] Disconnecting MQTT client...")
            mqtt_client.disconnect()
        except Exception as e:
            print(f"[WEB-MQTT] Error during listener cleanup: {e}")
    print("[WEB-MQTT] Listener thread finished.")


# --- Flask Web App ---
app = Flask(__name__)

@app.route('/')
def status_page():
    with state_lock:
        current_state = security_state
        flag_to_display = FINAL_FLAG if current_state == "GREEN" else None

    # Corrected HTML template with centering fix
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Prison Security Status</title>
        <meta http-equiv="refresh" content="3">
        <style>
            body { display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #222; color: white; font-family: monospace, sans-serif; }
            .content-wrapper { display: flex; flex-direction: column; align-items: center; }
            .status-light { width: 200px; height: 200px; border-radius: 50%; border: 10px solid #555; display: flex; justify-content: center; align-items: center; font-size: 24px; font-weight: bold; text-shadow: 2px 2px 4px rgba(0,0,0,0.5); margin-bottom: 20px; color: white; text-align: center; }
            .red { background: radial-gradient(circle, rgba(255,50,50,1) 0%, rgba(180,0,0,1) 100%); box-shadow: 0 0 30px #ff0000; }
            .green { background: radial-gradient(circle, rgba(50,255,50,1) 0%, rgba(0,180,0,1) 100%); box-shadow: 0 0 30px #00ff00; }
            .flag-box { margin-top: 10px; padding: 15px; background-color: #333; border: 1px solid #666; border-radius: 5px; font-size: 1.2em; text-align: center; }
        </style>
    </head>
    <body>
        <div class="content-wrapper"> {# Wrap content #}
            {% if state == "RED" %}
                <div class="status-light red">LOCKED<br>DOWN</div> {# Use <br> for line break #}
                <div>Status: Secure</div>
            {% else %}
                <div class="status-light green">SYSTEMS<br>DISABLED</div> {# Use <br> for line break #}
                <div>Status: Compromised!</div>
                <div class="flag-box">FLAG: {{ flag_value }}</div>
            {% endif %}
        </div>
    </body>
    </html>
    """
    return render_template_string(html_template, state=current_state, flag_value=flag_to_display)

# Add a basic shutdown function for Flask's dev server (doesn't work with production servers)
def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        print('Warning: Not running with the Werkzeug Server, cannot shutdown programmatically.')
        # For non-dev servers, you'd typically kill the process
    else:
        func()

@app.route('/shutdown', methods=['POST']) # Optional: for remote shutdown if needed
def shutdown():
    print("Shutdown requested via HTTP...")
    shutdown_server()
    return 'Server shutting down...'

# --- Main Execution ---
if __name__ == '__main__':
    # Start MQTT listener in background
    mqtt_thread = threading.Thread(target=mqtt_listener_thread, daemon=True)
    mqtt_thread.start()

    print(f"[*] Starting Flask web server on http://{WEB_HOST}:{WEB_PORT}")
    print("[*] This server shows the security status light.")
    print(f"[*] Monitoring MQTT topic '{TRIGGER_TOPIC}' for payload '{TRIGGER_PAYLOAD}'...")
    try:
        # Run Flask app (use werkzeug's built-in server for simplicity)
        # Turn off reloader to prevent running MQTT thread twice
        app.run(host=WEB_HOST, port=WEB_PORT, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print("\n[*] Shutdown signal received (KeyboardInterrupt)...")
    finally:
        print("[*] Signaling MQTT listener thread to exit...")
        exit_app.set() # Tell MQTT thread to stop its loop
        if mqtt_client:
            try: mqtt_client.disconnect() # Attempt disconnect from main thread too
            except Exception: pass
        print("[*] Waiting for MQTT listener thread to join...")
        mqtt_thread.join(timeout=3) # Wait max 3 seconds for thread cleanup
        if mqtt_thread.is_alive():
             print("[!] MQTT thread did not exit cleanly.")
        print("[*] Web server shut down.")
