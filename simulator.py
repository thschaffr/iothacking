# --- START OF FILE simulator.py ---

import json
import time
import paho.mqtt.client as mqtt # Keep for BrokerSettings protocol constants if needed
from pathlib import Path
from data_classes import BrokerSettings, ClientSettings
from topic import Topic
from listener import MQTTCommandListener # <-- IMPORT NEW LISTENER CLASS
import sys

class Simulator:
    def __init__(self, settings_file: Path):
        self.settings_file = settings_file
        self.default_client_settings = ClientSettings(
            clean=None, retain=False, qos=1, time_interval=10
        )
        self.topics: list[Topic] = []
        self.broker_settings: BrokerSettings = None
        self.listener: MQTTCommandListener = None # Attribute to hold the listener instance
        self._load_config()

        # --- Listener Configuration (Defined here, not in settings.json) ---
        self.command_target_topics = [
            "prison/celldoor/block_a",
            "prison/celldoor/block_b",
            "prison/security/control_room",
            "prison/power/grid_monitor",
            "prison/environment/temp/infirmary",
            "prison/guard/patrol_7"
        ]
        self.trigger_command = "GET_FLAG"
        self.flag_response = "{{SECURITY-DISARMED}}"
        self.flag_response_topic = "prison/system/flag_channel"
        # --------------------------------------------------------------------

    def _read_client_settings(self, settings_dict: dict, default: ClientSettings) -> ClientSettings:
        # ... (this method remains unchanged) ...
        qos_value = settings_dict.get('QOS', default.qos)
        return ClientSettings(
            clean=settings_dict.get('clean', settings_dict.get('CLEAN_SESSION', default.clean)),
            retain=settings_dict.get('RETAIN', default.retain),
            qos=qos_value,
            time_interval=settings_dict.get('TIME_INTERVAL', default.time_interval)
        )

    def _load_config(self):
        # ... (this method remains largely unchanged, just loads broker settings and prepares publisher topic list) ...
        print(f"INFO: Loading configuration from {self.settings_file}")
        try:
            with open(self.settings_file, 'r') as f: config = json.load(f)
        except Exception as e: print(f"ERROR reading settings: {e}"); return

        protocol_str = config.get('PROTOCOL_VERSION', '3.1.1')
        protocol_version = mqtt.MQTTv311
        if protocol_str == '5': protocol_version = mqtt.MQTTv5
        elif protocol_str == '3.1': protocol_version = mqtt.MQTTv31
        self.broker_settings = BrokerSettings(
            url=config.get('BROKER_URL', 'localhost'), port=config.get('BROKER_PORT', 1883),
            protocol=protocol_version, username=config.get('BROKER_USERNAME'), password=config.get('BROKER_PASSWORD')
        )
        if self.broker_settings.username is None and config.get('allow_anonymous') is False:
             print("WARNING: Missing BROKER_USERNAME.")
        broker_level_client_settings = self._read_client_settings(config, self.default_client_settings)
        loaded_topics = []
        for i, topic_config in enumerate(config.get('TOPICS', [])):
            topic_type = topic_config.get('TYPE'); prefix = topic_config.get('PREFIX')
            if not prefix or not topic_type: continue
            topic_data_config = topic_config.get('DATA', []); topic_payload_root = topic_config.get('PAYLOAD_ROOT', {})
            topic_client_settings = self._read_client_settings(topic_config, broker_level_client_settings)
            try:
                if topic_type == 'single':
                    loaded_topics.append(Topic(self.broker_settings, prefix, topic_data_config, topic_payload_root, topic_client_settings))
                elif topic_type == 'multiple':
                    start = topic_config.get('RANGE_START', 1); end = topic_config.get('RANGE_END', 0)
                    if end < start: continue
                    for item_id in range(start, end + 1):
                        loaded_topics.append(Topic(self.broker_settings, f"{prefix}/{item_id}", topic_data_config, topic_payload_root, topic_client_settings))
                elif topic_type == 'list':
                    item_list = topic_config.get('LIST', [])
                    if not item_list: continue
                    for item in item_list:
                         loaded_topics.append(Topic(self.broker_settings, f"{prefix}/{item}", topic_data_config, topic_payload_root, topic_client_settings))
                else: print(f"Warning: Unknown topic TYPE '{topic_type}'.")
            except KeyError as e: print(f"ERROR: Missing key {e} in topic config for '{prefix}'.")
            except Exception as e: print(f"ERROR: Init failed for '{prefix}': {e}.")
        if not loaded_topics: print("WARNING: No valid publisher topics loaded.")
        else: print(f"INFO: Successfully prepared {len(loaded_topics)} publisher topic instance(s).")
        self.topics = loaded_topics


    # --- Modified run and stop methods ---
    def run(self):
        """Starts listener and all configured publisher topic threads."""
        if not self.topics:
            print("ERROR: No publisher topics loaded to run. Exiting.")
            # Even if no publishers, maybe we want the listener? Decide based on use case.
            # For this CTF, probably exit if no publishers defined.
            return
        if not self.broker_settings:
             print("ERROR: Broker settings not loaded. Exiting.")
             return

        # --- Start Listener ---
        print("INFO: Creating MQTT Command Listener...")
        self.listener = MQTTCommandListener(
             broker_settings=self.broker_settings,
             command_target_topics=self.command_target_topics,
             trigger_command=self.trigger_command,
             flag_response=self.flag_response,
             flag_response_topic=self.flag_response_topic
        )
        self.listener.start()
        # Optional: Wait briefly for listener to attempt connection/subscription
        time.sleep(2) # Give listener time to connect/subscribe
        if not self.listener.is_alive():
             print("ERROR: Listener thread failed to start or exited prematurely. Stopping.")
             self.stop() # Attempt cleanup
             return
        # --------------------

        print(f"INFO: Starting {len(self.topics)} publisher thread(s) with delays...")
        for i, topic in enumerate(self.topics):
            print(f" -> Starting Publisher: {topic.topic_url}")
            topic.start()
            time.sleep(1.5) # Keep stagger delay

        print("INFO: All publisher threads started. Simulator running. Press Ctrl+C to stop.")

        try:
            # Main loop now just monitors threads
            while self.listener.is_alive() and any(t.is_alive() for t in self.topics):
                time.sleep(0.5)
            # If loop exits, either listener died or all publishers died
            if not self.listener.is_alive():
                print("ERROR: Listener thread appears to have stopped unexpectedly.")
            if not any(t.is_alive() for t in self.topics):
                 print("INFO: All publisher threads appear to have stopped.")

        except KeyboardInterrupt:
            print("\nINFO: KeyboardInterrupt received...")
        except Exception as e:
             print(f"\nERROR: Unexpected error in main simulator loop: {e}")
        finally:
            print("INFO: Main loop exited. Initiating shutdown sequence...")
            self.stop() # Stop publishers and listener
            print("INFO: Simulator main loop finished.")

    def stop(self):
        """Stops listener and all running publisher topic threads."""
        print("INFO: Stopping simulator components...")

        # --- Stop Listener ---
        if self.listener and self.listener.is_alive():
            print("INFO: Stopping Listener Thread...")
            self.listener.disconnect() # Signal listener to stop and disconnect
        # -------------------

        # --- Stop Publisher Threads ---
        print(f"INFO: Stopping {len(self.topics)} publisher thread(s)...")
        active_topics = [t for t in self.topics if t.is_alive()]
        if not active_topics: print("INFO: No active publisher threads to stop.")
        for topic in active_topics:
            print(f" -> Requesting stop: {topic.topic_url}")
            topic.disconnect() # disconnect() signals thread to stop

        print("INFO: Waiting for threads to terminate...")
        start_time = time.time(); shutdown_timeout = 10

        # Wait for listener first (optional, but good practice)
        if self.listener and self.listener.is_alive():
            print("INFO: Waiting for Listener thread to join...")
            self.listener.join(timeout=max(0.1, shutdown_timeout - (time.time() - start_time)))
            if self.listener.is_alive():
                 print("WARNING: Listener thread did not join.")

        # Wait for publishers
        print("INFO: Waiting for publisher threads to join...")
        threads_to_join = [t for t in self.topics if t.is_alive()]
        while threads_to_join:
             elapsed_time = time.time() - start_time; remaining_time = shutdown_timeout - elapsed_time
             if remaining_time <= 0: print("WARNING: Shutdown timeout reached for publishers."); break
             thread = threads_to_join[0]; thread.join(timeout=max(0.1, remaining_time))
             if thread.is_alive(): print(f"WARNING: Publisher thread {thread.topic_url} did not stop gracefully."); threads_to_join.pop(0)
             else: threads_to_join.pop(0)

        # Final check
        remaining_publishers = [t.topic_url for t in self.topics if t.is_alive()]
        listener_alive = self.listener and self.listener.is_alive()
        if remaining_publishers or listener_alive:
             print(f"WARNING: Some threads may still be running (Listener: {listener_alive}, Publishers: {remaining_publishers})")
        else:
            print("INFO: All monitored threads have terminated.")

        print("INFO: Simulator stop sequence complete.")

# --- END OF FILE simulator.py ---
