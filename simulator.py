# --- START OF simulator.py ---

import json
import time
import paho.mqtt.client as mqtt
from pathlib import Path
from data_classes import BrokerSettings, ClientSettings
from topic import Topic

class Simulator:
    def __init__(self, settings_file: Path):
        self.settings_file = settings_file
        # --- REVERT QOS DEFAULT ---
        self.default_client_settings = ClientSettings(
            clean=None,
            retain=False,
            qos=1,           # <-- REVERTED back to 1 (or your original desired default)
            time_interval=10
        )
        # --------------------------
        self.topics: list[Topic] = []
        self._load_config()

    def _read_client_settings(self, settings_dict: dict, default: ClientSettings) -> ClientSettings:
        # Read QoS from settings, defaulting to the class default (now 1 again)
        qos_value = settings_dict.get('QOS', default.qos)
        return ClientSettings(
            clean=settings_dict.get('clean', settings_dict.get('CLEAN_SESSION', default.clean)),
            retain=settings_dict.get('RETAIN', default.retain),
            qos=qos_value,
            time_interval=settings_dict.get('TIME_INTERVAL', default.time_interval)
        )

    # --- _load_config method remains the same as the previous version ---
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
        protocol_str = config.get('PROTOCOL_VERSION', '3.1.1')
        protocol_version = mqtt.MQTTv311
        if protocol_str == '5': protocol_version = mqtt.MQTTv5
        elif protocol_str == '3.1': protocol_version = mqtt.MQTTv31
        elif protocol_str != '3.1.1': print(f"WARNING: Unknown PROTOCOL_VERSION '{protocol_str}'. Using default.")
        broker_settings = BrokerSettings(
            url=config.get('BROKER_URL', 'localhost'),
            port=config.get('BROKER_PORT', 1883),
            protocol=protocol_version,
            username=config.get('BROKER_USERNAME'),
            password=config.get('BROKER_PASSWORD')
        )
        if broker_settings.username is None and config.get('allow_anonymous') is False:
             print("WARNING: Missing BROKER_USERNAME.")
        broker_level_client_settings = self._read_client_settings(config, self.default_client_settings)
        loaded_topics = []
        for i, topic_config in enumerate(config.get('TOPICS', [])):
            topic_type = topic_config.get('TYPE'); prefix = topic_config.get('PREFIX')
            if not prefix or not topic_type: print(f"Warning: Topic {i} missing PREFIX/TYPE."); continue
            topic_data_config = topic_config.get('DATA', []); topic_payload_root = topic_config.get('PAYLOAD_ROOT', {})
            topic_client_settings = self._read_client_settings(topic_config, broker_level_client_settings)
            try:
                if topic_type == 'single':
                    loaded_topics.append(Topic(broker_settings, prefix, topic_data_config, topic_payload_root, topic_client_settings))
                elif topic_type == 'multiple':
                    start = topic_config.get('RANGE_START', 1); end = topic_config.get('RANGE_END', 0)
                    if end < start: print(f"Warning: Invalid range for '{prefix}'."); continue
                    for item_id in range(start, end + 1):
                        loaded_topics.append(Topic(broker_settings, f"{prefix}/{item_id}", topic_data_config, topic_payload_root, topic_client_settings))
                elif topic_type == 'list':
                    item_list = topic_config.get('LIST', [])
                    if not item_list: print(f"Warning: Empty list for '{prefix}'."); continue
                    for item in item_list:
                         loaded_topics.append(Topic(broker_settings, f"{prefix}/{item}", topic_data_config, topic_payload_root, topic_client_settings))
                else: print(f"Warning: Unknown topic TYPE '{topic_type}'.")
            except KeyError as e: print(f"ERROR: Missing key {e} in topic config for '{prefix}'.")
            except Exception as e: print(f"ERROR: Init failed for '{prefix}': {e}.")
        if not loaded_topics: print("WARNING: No valid topics loaded.")
        else: print(f"INFO: Successfully prepared {len(loaded_topics)} topic instance(s).")
        self.topics = loaded_topics

    # --- run method keeps the stagger delay ---
    def run(self):
        """Starts all configured topic threads with a delay and waits."""
        if not self.topics:
            print("ERROR: No topics loaded to run. Exiting.")
            return
        print(f"INFO: Starting {len(self.topics)} topic thread(s) with delays...")
        for i, topic in enumerate(self.topics):
            print(f" -> Starting: {topic.topic_url}")
            topic.start()
            # --- KEEP INCREASED DELAY ---
            time.sleep(1.5) # Keep delay at 1.5 seconds
            # --------------------------
        print("INFO: All topic threads started. Simulator running. Press Ctrl+C to stop.")
        try:
            while any(t.is_alive() for t in self.topics):
                time.sleep(0.5)
        except KeyboardInterrupt: print("\nINFO: KeyboardInterrupt received...")
        except Exception as e: print(f"\nERROR: Unexpected error in main loop: {e}")
        finally:
            print("INFO: Main loop exited. Initiating shutdown sequence...")
            self.stop()
            print("INFO: Simulator main loop finished.")

    # --- stop method remains the same as the previous version ---
    def stop(self):
        """Requests all running topic threads to stop and waits for them."""
        print(f"INFO: Stopping {len(self.topics)} topic thread(s)...")
        active_topics = [t for t in self.topics if t.is_alive()]
        if not active_topics: print("INFO: No active threads to stop.")
        for topic in active_topics: print(f" -> Requesting stop: {topic.topic_url}"); topic.disconnect()
        print("INFO: Waiting for threads to terminate...")
        start_time = time.time(); shutdown_timeout = 10
        threads_to_join = [t for t in self.topics if t.is_alive()]
        while threads_to_join:
             elapsed_time = time.time() - start_time; remaining_time = shutdown_timeout - elapsed_time
             if remaining_time <= 0: print("WARNING: Shutdown timeout reached."); break
             thread = threads_to_join[0]; thread.join(timeout=max(0.1, remaining_time))
             if thread.is_alive(): print(f"WARNING: Thread {thread.topic_url} did not stop gracefully."); threads_to_join.pop(0)
             else: threads_to_join.pop(0)
        remaining_alive = [t.topic_url for t in self.topics if t.is_alive()]
        if remaining_alive: print(f"WARNING: Threads still running: {remaining_alive}")
        else: print("INFO: All monitored threads have terminated.")
        print("INFO: Simulator stop sequence complete.")

# --- END OF FILE simulator.py ---
