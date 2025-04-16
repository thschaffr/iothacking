# --- START OF FILE topic.py ---

import time # Ensure time is imported
import json
import threading
import paho.mqtt.client as mqtt
import random
import math # Needed for math_expression example
from typing import Optional, List, Dict # Import necessary types

# Import your specific data classes
from data_classes import BrokerSettings, ClientSettings

# ===============================================================
# Placeholder/Example Data Generator Classes
# (Assuming these are correct as per previous versions)
# ===============================================================
class TopicDataBase:
    def __init__(self, config: dict):
        self.name = config['NAME']
        self.type = config['TYPE']
        self.retain_probability = config.get('RETAIN_PROBABILITY', 0.0)
        self.is_active = True
    def generate_value(self): raise NotImplementedError
    def should_retain(self) -> bool: return random.random() < self.retain_probability

class TopicDataNumber(TopicDataBase):
    def __init__(self, config):
        super().__init__(config)
        self.min_value = config['MIN_VALUE']
        self.max_value = config['MAX_VALUE']
        self.current_value = config.get('INITIAL_VALUE', self.min_value)
        self.current_value = max(self.min_value, min(self.max_value, self.current_value))
        self.max_step = config['MAX_STEP']
        self.increase_probability = config.get('INCREASE_PROBABILITY', 0.5)
        self.reset_probability = config.get('RESET_PROBABILITY', 0.0)
        self.restart_on_boundaries = config.get('RESTART_ON_BOUNDARIES', False)
        self.initial_value = config.get('INITIAL_VALUE')
    def generate_value(self):
        if self.initial_value is not None and random.random() < self.reset_probability:
             self.current_value = self.initial_value
             self.current_value = max(self.min_value, min(self.max_value, self.current_value))
        else:
            step = random.uniform(0, self.max_step)
            increase = random.random() < self.increase_probability
            if increase:
                self.current_value += step
                if self.current_value > self.max_value:
                    self.current_value = self.initial_value if self.restart_on_boundaries and self.initial_value is not None else self.max_value
            else:
                self.current_value -= step
                if self.current_value < self.min_value:
                    self.current_value = self.initial_value if self.restart_on_boundaries and self.initial_value is not None else self.min_value
        return int(round(self.current_value)) if self.type == 'int' else round(float(self.current_value), 4)

class TopicDataBool(TopicDataBase):
     def __init__(self, config): super().__init__(config); self.current_value = random.choice([True, False])
     def generate_value(self): self.current_value = not self.current_value; return self.current_value

class TopicDataRawValue(TopicDataBase):
    def __init__(self, config):
        super().__init__(config); self.values = config.get('VALUES', []); self.restart_on_end = config.get('RESTART_ON_END', False)
        self.value_default = config.get('VALUE_DEFAULT'); self.current_index = 0
        if not self.values: print(f"Warning: Raw values list empty for '{self.name}'."); self.is_active = False
    def generate_value(self):
        if not self.is_active or not self.values: return None
        if self.current_index >= len(self.values):
            if self.restart_on_end: self.current_index = 0
            else: print(f"Info: End of non-restarting raw values for '{self.name}'."); self.is_active = False; return None
        value = self.values[self.current_index]
        if isinstance(value, dict) and isinstance(self.value_default, dict): value = {**self.value_default, **value} # Python 3.5+ dict merge
        self.current_index += 1; return value

class TopicDataMathExpression(TopicDataBase):
     def __init__(self, config):
        super().__init__(config); self.expression = config.get('MATH_EXPRESSION', 'x'); self.interval_start = config.get('INTERVAL_START', 0.0)
        self.interval_end = config.get('INTERVAL_END', 1.0); self.min_delta = config.get('MIN_DELTA', 0.1); self.max_delta = config.get('MAX_DELTA', 0.1)
        self.x = float(self.interval_start); print(f"INFO: Math expression for '{self.name}' uses eval()."); self._compiled_expr = None
        try: self._compiled_expr = compile(self.expression, f"<string>-{self.name}", "eval")
        except Exception as e: print(f"ERROR: Compile failed for '{self.name}': {e}."); self.is_active = False
     def generate_value(self):
         if not self.is_active or self._compiled_expr is None: return None
         delta = random.uniform(self.min_delta, self.max_delta); self.x += delta
         if self.x > self.interval_end: self.x = self.interval_start + (self.x - self.interval_end) # Wrap around
         try:
             result = eval(self._compiled_expr, {"math": math}, {"x": self.x})
             return round(result, 4) if isinstance(result, (int, float)) else None
         except Exception as e: print(f"ERROR evaluating math for '{self.name}' (x={self.x}): {e}"); self.is_active = False; return None

# ===============================================================
# Topic Thread Class
# ===============================================================

class Topic(threading.Thread):
    def __init__(self,
                 broker_settings: BrokerSettings,
                 topic_url: str,
                 topic_data_config: List[dict],
                 topic_payload_root: Dict,
                 client_settings: ClientSettings):
        threading.Thread.__init__(self, daemon=True)

        self.broker_settings = broker_settings
        self.topic_url = topic_url
        self.topic_data = self._load_topic_data(topic_data_config)
        self.topic_payload_root = topic_payload_root or {}
        self.client_settings = client_settings

        self._stop_event = threading.Event()
        self.client: Optional[mqtt.Client] = None

    def _load_topic_data(self, topic_data_configs: List[dict]) -> List[TopicDataBase]:
        """Loads and initializes data generator instances based on config."""
        generators = []
        for config in topic_data_configs:
            data_type = config.get('TYPE', '').lower()
            try:
                GenClass = {
                    'int': TopicDataNumber, 'float': TopicDataNumber, 'bool': TopicDataBool,
                    'raw_values': TopicDataRawValue, 'math_expression': TopicDataMathExpression
                }.get(data_type)
                if GenClass: generators.append(GenClass(config))
                else: print(f"Warning: Unknown data TYPE '{config.get('TYPE')}' for item '{config.get('NAME', 'N/A')}' in topic '{self.topic_url}'.")
            except KeyError as e: print(f"ERROR: Missing key {e} in data config for '{config.get('NAME', 'N/A')}' in topic '{self.topic_url}'.")
            except Exception as e: print(f"ERROR: Init failed for '{config.get('NAME', 'N/A')}' in topic '{self.topic_url}': {e}")
        return generators

    def _connect(self) -> bool:
        """Establishes connection to the MQTT broker."""
        if self.client and self.client.is_connected():
             return True

        base_client_id = f"simulator-{self.topic_url.replace('/', '_')}"
        client_id = f"{base_client_id}-{random.randint(1000,9999)}" if self.client_settings.clean is not False else base_client_id

        try:
            if self.broker_settings.protocol >= mqtt.MQTTv5:
                self.client = mqtt.Client(client_id=client_id, protocol=self.broker_settings.protocol)
            else:
                use_clean_session = True if self.client_settings.clean is None else self.client_settings.clean
                self.client = mqtt.Client(client_id=client_id, protocol=self.broker_settings.protocol, clean_session=use_clean_session)

            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_publish = self._on_publish

            if self.broker_settings.username:
                self.client.username_pw_set(self.broker_settings.username, self.broker_settings.password)

            self.client.connect(self.broker_settings.url, self.broker_settings.port, keepalive=60)
            self.client.loop_start()
            # We might need a very brief pause here too AFTER loop_start,
            # but let's try the pause in run() first.
            # time.sleep(0.1)
            return True
        except Exception as e:
             print(f"ERROR: Connect failed for {self.topic_url}: {e}")
             self.client = None
             return False

    def disconnect(self):
        """Signals the thread to stop and cleans up the MQTT client."""
        if not self._stop_event.is_set():
             self._stop_event.set()

        client_instance = self.client
        if client_instance:
            try:
                 client_instance.loop_stop()
                 # Short timeout before disconnect to allow loop stop processing
                 time.sleep(0.1)
                 client_instance.disconnect()
            except Exception as e:
                 if not self._stop_event.is_set(): # Avoid warnings during normal stop
                     print(f"WARNING: Exception during disconnect for {self.topic_url}: {e}")
            finally:
                 self.client = None


    # ================================================================
    # --- MODIFIED run Method starts here ---
    # ================================================================
    def run(self):
        """Main thread loop: connect, generate data, publish, wait."""
        if not self._connect():
            print(f"ERROR: Initial connection failed for {self.topic_url}. Thread exiting.")
            return # Exit thread if initial connection fails

        # --- ADDED STABILIZATION PAUSE HERE ---
        # Give the connection and background loop thread time to settle
        print(f"INFO: Stabilizing connection for {self.topic_url}...")
        time.sleep(0.5) # Wait for 0.5 seconds
        # ------------------------------------

        while not self._stop_event.is_set():
            current_client = self.client
            if not current_client:
                if not self._stop_event.is_set():
                    print(f"ERROR: Client object for {self.topic_url} is None. Stopping thread.")
                break

            # Check connection status more carefully
            is_conn = False
            try:
                # Check the actual socket connection state if possible (may vary by paho version)
                # For robustness, rely primarily on is_connected() for now
                is_conn = current_client.is_connected()
            except Exception as conn_err:
                # If checking connection fails, assume disconnected
                 if not self._stop_event.is_set():
                     print(f"ERROR: Checking connection status failed for {self.topic_url}: {conn_err}. Stopping thread.")
                 break # Exit if connection check fails

            if not is_conn:
                if not self._stop_event.is_set():
                    print(f"WARNING: Client for {self.topic_url} detected disconnected in loop. Stopping thread.")
                break

            try:
                payload_data = self._generate_payload()
                if payload_data is None:
                    if not self._stop_event.is_set():
                         print(f"INFO: No active data for {self.topic_url}. Stopping thread.")
                    break

                payload_json = json.dumps(payload_data, indent=None)
                retain_message = self.client_settings.retain
                if not retain_message:
                    for data_gen in self.topic_data:
                        if data_gen.is_active and hasattr(data_gen, 'should_retain') and data_gen.should_retain():
                            retain_message = True; break

                print(f"DEBUG: Publishing to {self.topic_url}: {payload_json} (QoS: {self.client_settings.qos}, Retain: {retain_message})")

                msg_info = current_client.publish(
                    topic=self.topic_url, payload=payload_json,
                    qos=self.client_settings.qos, retain=retain_message
                )

                # If using QoS 1 or 2, check if publish succeeded (optional)
                # if self.client_settings.qos > 0:
                #    try:
                #        msg_info.wait_for_publish(timeout=5.0)
                #    except Exception as pub_e:
                #        if not self._stop_event.is_set():
                #             print(f"WARNING: Publish confirmation failed/timed out for {self.topic_url}: {pub_e}")
                         # Decide whether to break or continue here


                self._stop_event.wait(self.client_settings.time_interval)

            except Exception as e:
                log_error = True
                if self._stop_event.is_set():
                    if isinstance(e, (mqtt.MQTTException, AttributeError, RuntimeError, ConnectionError)):
                        log_error = False # Likely due to disconnection during shutdown

                if log_error:
                    # Print exception type and message for better debugging
                    print(f"ERROR: Unhandled exception in run loop for {self.topic_url}: {type(e).__name__}: {e}")
                    # Optionally add traceback:
                    # import traceback
                    # traceback.print_exc()
                break # Exit loop on major errors


        # Cleanup after loop exit
        self.disconnect()
    # ================================================================
    # --- MODIFIED run Method ends here ---
    # ================================================================


    def _generate_payload(self) -> Optional[Dict]:
        """Generates the complete payload dictionary for one message."""
        payload: Dict = {}
        payload.update(self.topic_payload_root)
        has_active_generator = False; active_generators_count = 0
        for data_gen in self.topic_data:
            if data_gen.is_active:
                active_generators_count += 1
                try:
                    value = data_gen.generate_value()
                    if value is not None: payload[data_gen.name] = value; has_active_generator = True
                except Exception as e: print(f"ERROR: Generating value for '{data_gen.name}' on {self.topic_url}: {e}"); data_gen.is_active = False
        if active_generators_count == 0 and not self.topic_payload_root: return None
        return payload

    # --- MQTT Callbacks ---
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connection attempt completes."""
        if rc != 0:
            error_msg = f"Connection failed for {self.topic_url} code {rc}"
            try: error_msg += f" ({mqtt.connack_string(rc)})"
            except ValueError: error_msg += " (Unknown reason)"
            print(f"ERROR: {error_msg}")
            self._stop_event.set() # Signal stop if connection fails

    def _on_disconnect(self, client, userdata, rc):
        """Callback when the client disconnects."""
        if not self._stop_event.is_set(): # Only log if unexpected
             if rc == 0: pass # Normal disconnect initiated by client or broker?
             else: print(f"WARNING: Unexpected disconnect for {self.topic_url} (rc={rc}).")
             self._stop_event.set() # Signal stop on unexpected disconnect

    def _on_publish(self, client, userdata, mid): pass # Reduce noise

# --- END OF FILE topic.py ---
