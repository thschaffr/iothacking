# --- START OF FILE simulator.py ---

import json
import time # <-- ADD THIS IMPORT AT THE TOP
import paho.mqtt.client as mqtt # Import for protocol constants
from pathlib import Path

# Use the correct import path for your data classes package
from data_classes import BrokerSettings, ClientSettings
from topic import Topic # Import your Topic class

class Simulator:
    def __init__(self, settings_file: Path):
        self.settings_file = settings_file
        # Default client settings (can be overridden at broker or topic level)
        self.default_client_settings = ClientSettings(
            clean=None,      # Default clean session behavior depends on MQTT protocol
            retain=False,    # Default: messages are not retained
            qos=1,           # Default: QoS level 1
            time_interval=10 # Default: 10 seconds between messages
        )
        self.topics: list[Topic] = [] # List to hold running Topic threads
        self._load_config() # Load config during initialization

    def _read_client_settings(self, settings_dict: dict, default: ClientSettings) -> ClientSettings:
        """Reads client-specific settings, falling back to defaults."""
        return ClientSettings(
            # Read 'CLEAN_SESSION' for backward compatibility, prefer 'clean'
            clean=settings_dict.get('clean', settings_dict.get('CLEAN_SESSION', default.clean)),
            retain=settings_dict.get('RETAIN', default.retain),
            qos=settings_dict.get('QOS', default.qos),
            time_interval=settings_dict.get('TIME_INTERVAL', default.time_interval)
        )

    def _load_config(self):
        """Loads configuration from the settings file and prepares topics."""
        print(f"INFO: Loading configuration from {self.settings_file}")
        try:
            with open(self.settings_file, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            print(f"ERROR: Settings file not found: {self.settings_file}")
            return
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in settings file {self.settings_file}: {e}")
            return
        except Exception as e:
            print(f"ERROR: Failed to read settings file {self.settings_file}: {e}")
            return

        # --- Broker Settings ---
        protocol_str = config.get('PROTOCOL_VERSION', '3.1.1')
        protocol_version = mqtt.MQTTv311 # Default
        if protocol_str == '5':
            protocol_version = mqtt.MQTTv5
        elif protocol_str == '3.1':
             protocol_version = mqtt.MQTTv31
        elif protocol_str != '3.1.1':
             print(f"WARNING: Unknown PROTOCOL_VERSION '{protocol_str}'. Using default MQTTv3.1.1.")


        broker_settings = BrokerSettings(
            url=config.get('BROKER_URL', 'localhost'),
            port=config.get('BROKER_PORT', 1883),
            protocol=protocol_version,
            # --- Read Auth Credentials ---
            username=config.get('BROKER_USERNAME'), # Returns None if key missing
            password=config.get('BROKER_PASSWORD')
        )

        # Check if auth is configured on broker but missing in settings
        if broker_settings.username is None and config.get('allow_anonymous') is False:
             print("WARNING: Broker likely requires authentication (based on allow_anonymous=false hint)," \
                   " but BROKER_USERNAME is not set in settings.")

        # --- Default Client Settings at Broker Level ---
        # These apply to all topics unless overridden in the topic's config
        broker_level_client_settings = self._read_client_settings(config, self.default_client_settings)

        # --- Load Topics ---
        loaded_topics = []
        for i, topic_config in enumerate(config.get('TOPICS', [])):
            topic_type = topic_config.get('TYPE')
            prefix = topic_config.get('PREFIX')
            if not prefix:
                 print(f"Warning: Topic config at index {i} is missing 'PREFIX'. Skipping.")
                 continue
            if not topic_type:
                 print(f"Warning: Topic config for prefix '{prefix}' is missing 'TYPE'. Skipping.")
                 continue

            topic_data_config = topic_config.get('DATA', [])
            topic_payload_root = topic_config.get('PAYLOAD_ROOT', {})

            # Topic-specific client settings override broker-level settings
            topic_client_settings = self._read_client_settings(topic_config, broker_level_client_settings)

            try:
                if topic_type == 'single':
                    topic_url = prefix
                    loaded_topics.append(Topic(broker_settings, topic_url, topic_data_config, topic_payload_root, topic_client_settings))
                elif topic_type == 'multiple':
                    start = topic_config.get('RANGE_START', 1)
                    end = topic_config.get('RANGE_END', 0)
                    if end < start:
                         print(f"Warning: Invalid range for multiple topic '{prefix}' (start={start}, end={end}). Skipping.")
                         continue
                    for item_id in range(start, end + 1):
                        topic_url = f"{prefix}/{item_id}"
                        loaded_topics.append(Topic(broker_settings, topic_url, topic_data_config, topic_payload_root, topic_client_settings))
                elif topic_type == 'list':
                    item_list = topic_config.get('LIST', [])
                    if not item_list:
                         print(f"Warning: Empty list for list topic '{prefix}'. Skipping.")
                         continue
                    for item in item_list:
                        topic_url = f"{prefix}/{item}"
                        loaded_topics.append(Topic(broker_settings, topic_url, topic_data_config, topic_payload_root, topic_client_settings))
                else:
                    print(f"Warning: Unknown topic TYPE '{topic_type}' for prefix '{prefix}'. Skipping.")

            except KeyError as e:
                 print(f"ERROR: Missing required key {e} in topic config for prefix '{prefix}'. Skipping topic.")
            except Exception as e:
                 print(f"ERROR: Failed to initialize topic for prefix '{prefix}': {e}. Skipping topic.")


        if not loaded_topics:
            print("WARNING: No valid topics were loaded from the configuration.")
        else:
             print(f"INFO: Successfully prepared {len(loaded_topics)} topic instances.")

        self.topics = loaded_topics

    # ================================================================
    # --- MODIFIED run Method starts here ---
    # ================================================================
    def run(self):
        """Starts all configured topic threads with a delay and waits."""
        if not self.topics:
            print("ERROR: No topics loaded to run. Exiting.")
            return

        print(f"INFO: Starting {len(self.topics)} topic thread(s) with delays...")
        for i, topic in enumerate(self.topics):
            print(f" -> Starting: {topic.topic_url}")
            topic.start()
            # --- ADDED DELAY ---
            # Introduce a small pause between starting each thread
            # to reduce the chance of resource contention or race conditions
            # during initial connection flood. Adjust delay if needed (e.g., 0.2 or 1.0)
            time.sleep(0.5)
            # -------------------

        print("INFO: All topic threads started. Simulator running. Press Ctrl+C to stop.")

        # Keep main thread alive while topic threads are running
        try:
            # Check periodically if any threads are still alive
            while any(t.is_alive() for t in self.topics):
                time.sleep(0.5) # Check every half second
        except KeyboardInterrupt:
            print("\nINFO: KeyboardInterrupt received. Stopping simulator...")
            # self.stop() will be called in the finally block
        except Exception as e:
             print(f"\nERROR: Unexpected error in main simulator loop: {e}")
             # self.stop() will be called in the finally block
        finally:
            # Ensure graceful shutdown is attempted regardless of how the loop exited
            print("INFO: Main loop exited. Initiating shutdown sequence...")
            self.stop()
            print("INFO: Simulator main loop finished.")
    # ================================================================
    # --- MODIFIED run Method ends here ---
    # ================================================================

    def stop(self):
        """Requests all running topic threads to stop and waits for them."""
        print(f"INFO: Stopping {len(self.topics)} topic thread(s)...")
        # Request all threads to stop first
        active_topics = [t for t in self.topics if t.is_alive()]
        if not active_topics:
            print("INFO: No active threads to stop.")
            #return # Keep going to print final message

        for topic in active_topics:
            print(f" -> Requesting stop: {topic.topic_url}")
            topic.disconnect() # disconnect() now signals the stop event and handles client disconnect

        # Wait for threads to actually finish
        print("INFO: Waiting for threads to terminate...")
        start_time = time.time()
        shutdown_timeout = 10 # seconds

        # Create a list of threads we are waiting for
        threads_to_join = [t for t in self.topics if t.is_alive()]

        while threads_to_join:
             # Calculate remaining time
             elapsed_time = time.time() - start_time
             remaining_time = shutdown_timeout - elapsed_time
             if remaining_time <= 0:
                 print("WARNING: Shutdown timeout reached.")
                 break # Stop waiting

             # Join the first thread in the list with the remaining timeout
             thread = threads_to_join[0]
             thread.join(timeout=max(0.1, remaining_time)) # Ensure timeout is positive

             if thread.is_alive():
                 # If timeout reached for this thread, move on (it might be stuck)
                 print(f"WARNING: Thread for {thread.topic_url} did not stop gracefully within timeout.")
                 threads_to_join.pop(0) # Remove it from list to avoid infinite loop
             else:
                 # Thread finished, remove it and continue waiting for others
                 threads_to_join.pop(0)


        # Final check
        remaining_alive = [t.topic_url for t in self.topics if t.is_alive()]
        if remaining_alive:
             print(f"WARNING: The following threads may still be running: {remaining_alive}")
        else:
            print("INFO: All monitored threads have terminated.")

        print("INFO: Simulator stop sequence complete.")

# --- END OF FILE simulator.py ---
