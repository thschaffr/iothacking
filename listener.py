# --- START OF FILE listener.py ---

import threading
import time
import paho.mqtt.client as mqtt
from typing import List
from data_classes import BrokerSettings # Assuming BrokerSettings is in data_classes

class MQTTCommandListener(threading.Thread):
    """
    An MQTT client thread that listens on specific topics for a command
    and publishes a predefined response upon receiving it.
    """
    def __init__(self,
                 broker_settings: BrokerSettings,
                 command_target_topics: List[str],
                 trigger_command: str,
                 flag_response: str,
                 flag_response_topic: str,
                 listener_id: str = "simulator-listener"):
        """
        Initializes the listener thread.

        Args:
            broker_settings: Dataclass containing broker connection details.
            command_target_topics: List of topics to subscribe to for the command.
            trigger_command: The exact string payload that triggers the flag response.
            flag_response: The string payload (the flag) to publish.
            flag_response_topic: The topic to publish the flag response on.
            listener_id: The MQTT client ID for this listener instance.
        """
        threading.Thread.__init__(self, daemon=True) # Daemon threads exit if main exits
        self.broker_settings = broker_settings
        self.command_target_topics = command_target_topics
        self.trigger_command = trigger_command
        self.flag_response = flag_response
        self.flag_response_topic = flag_response_topic
        self.listener_id = listener_id

        self.client: mqtt.Client = None
        self._stop_event = threading.Event()
        self._subscribed = threading.Event() # Event to signal successful subscription

    # --- MQTT Callbacks ---
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for successful connection. Subscribes to target topics."""
        if rc == 0:
            print(f"INFO: Listener '{self.listener_id}' connected successfully.")
            # Subscribe to target topics
            subs = []
            for topic in self.command_target_topics:
                subs.append((topic, 1)) # Subscribe with QoS 1
            if subs:
                result, mid = client.subscribe(subs)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    # Note: Subscription success isn't guaranteed until SUBACK is received
                    # We rely on on_subscribe callback or just proceed optimistically here.
                    print(f"INFO: Listener '{self.listener_id}' subscription request sent for {len(subs)} topics (MID: {mid}).")
                    # Consider setting self._subscribed here or in on_subscribe if needed later
                    self._subscribed.set() # Assume success for now
                else:
                    print(f"ERROR: Listener '{self.listener_id}' subscription failed with code {result}")
                    self._subscribed.clear() # Mark as not subscribed
            else:
                print(f"WARNING: No command target topics defined for listener '{self.listener_id}'.")
                self._subscribed.set() # No subs needed, consider it "ready"
        else:
            error_msg = f"ERROR: Listener '{self.listener_id}' connection failed code {rc}"
            try: error_msg += f" ({mqtt.connack_string(rc)})"
            except ValueError: error_msg += " (Unknown reason)"
            print(error_msg)
            self._subscribed.clear()
            self._stop_event.set() # Stop thread if connection fails

    def _on_message(self, client, userdata, msg):
        """Callback for processing received messages."""
        try:
            payload = msg.payload.decode("utf-8")
            topic = msg.topic
            # print(f"DEBUG: Listener '{self.listener_id}' received on '{topic}': {payload}") # Optional debug

            # Check if the topic is relevant and payload matches command
            if topic in self.command_target_topics and payload == self.trigger_command:
                print(f"INFO: Listener '{self.listener_id}' received '{self.trigger_command}' on '{topic}'. Publishing flag...")
                # Publish the flag
                pub_result, _ = self.client.publish(
                    self.flag_response_topic,
                    payload=self.flag_response,
                    qos=1,
                    retain=False
                )
                if pub_result == mqtt.MQTT_ERR_SUCCESS:
                    print(f"INFO: Listener '{self.listener_id}' published flag '{self.flag_response}' successfully to '{self.flag_response_topic}'.")
                else:
                    print(f"ERROR: Listener '{self.listener_id}' failed to publish flag. Code: {pub_result}")

        except UnicodeDecodeError:
            print(f"WARNING: Listener '{self.listener_id}' received non-UTF8 payload on topic '{msg.topic}'")
        except Exception as e:
            print(f"ERROR: Listener '{self.listener_id}' error processing message: {e}")

    def _on_disconnect(self, client, userdata, rc):
        """Callback for disconnections."""
        self._subscribed.clear() # Clear subscribed status on disconnect
        if rc != 0 and not self._stop_event.is_set():
             print(f"WARNING: Listener '{self.listener_id}' unexpectedly disconnected (rc={rc}).")
             # Note: No automatic reconnect implemented here for simplicity.
             # Could add reconnect logic here if needed.
             self._stop_event.set() # Signal thread to stop on unexpected disconnect

    def _connect(self) -> bool:
        """Creates and connects the MQTT client."""
        try:
            self.client = mqtt.Client(client_id=self.listener_id, protocol=self.broker_settings.protocol)
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            self.client.on_disconnect = self._on_disconnect
            # Optional: Add on_subscribe if more robust subscription check needed
            # self.client.on_subscribe = self._on_subscribe

            if self.broker_settings.username:
                self.client.username_pw_set(self.broker_settings.username, self.broker_settings.password)

            print(f"INFO: Connecting listener '{self.listener_id}'...")
            self.client.connect(self.broker_settings.url, self.broker_settings.port, keepalive=60)
            self.client.loop_start()
            return True
        except Exception as e:
            print(f"ERROR: Listener '{self.listener_id}' connection setup failed: {e}")
            self.client = None
            return False

    def disconnect(self):
        """Signals the thread to stop and disconnects the client."""
        if not self._stop_event.is_set():
            print(f"INFO: Disconnecting listener '{self.listener_id}'...")
            self._stop_event.set() # Signal run loop to exit

        client_instance = self.client
        if client_instance:
            try:
                client_instance.loop_stop()
                time.sleep(0.1) # Short pause before disconnect
                # Check if connected before attempting disconnect
                # if client_instance.is_connected(): # is_connected might not be reliable during shutdown
                client_instance.disconnect()
                print(f"INFO: Listener '{self.listener_id}' disconnected command sent.")
            except Exception as e:
                # Suppress errors during shutdown
                # print(f"WARNING: Listener '{self.listener_id}' exception during disconnect: {e}")
                pass
            finally:
                 self.client = None

    def run(self):
        """Main loop for the listener thread."""
        print(f"INFO: Starting listener thread '{self.listener_id}'.")
        if not self._connect():
            print(f"ERROR: Listener '{self.listener_id}' failed initial connection. Thread exiting.")
            return

        # Wait until stopped
        self._stop_event.wait()

        # Cleanup
        print(f"INFO: Listener thread '{self.listener_id}' received stop signal. Cleaning up.")
        self.disconnect()
        print(f"INFO: Listener thread '{self.listener_id}' finished.")

# --- END OF FILE listener.py ---
