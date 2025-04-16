# --- START OF FILE topic.py ---

import time
import json
import threading
import paho.mqtt.client as mqtt
import random
import math # Needed for math_expression example
from typing import Optional, List, Dict # <-- IMPORT ADDED HERE

# Import your specific data classes
from data_classes import BrokerSettings, ClientSettings

# ===============================================================
# Placeholder/Example Data Generator Classes
# Replace these with your actual data generation logic if needed
# (e.g., import them from a separate topic_data.py)
# ===============================================================
class TopicDataBase:
    def __init__(self, config: dict):
        self.name = config['NAME']
        self.type = config['TYPE']
        # Default 0 means never retain based on probability unless topic default is True
        self.retain_probability = config.get('RETAIN_PROBABILITY', 0.0)
        self.is_active = True # Assume active unless generation fails or logic changes

    def generate_value(self):
        raise NotImplementedError("generate_value must be implemented in subclass")

    def should_retain(self) -> bool:
        """Determines if this specific data point suggests retention."""
        return random.random() < self.retain_probability

class TopicDataNumber(TopicDataBase):
    def __init__(self, config):
        super().__init__(config)
        self.min_value = config['MIN_VALUE']
        self.max_value = config['MAX_VALUE']
        self.current_value = config.get('INITIAL_VALUE', self.min_value)
        # Ensure initial value is within bounds
        self.current_value = max(self.min_value, min(self.max_value, self.current_value))
        self.max_step = config['MAX_STEP']
        self.increase_probability = config.get('INCREASE_PROBABILITY', 0.5)
        self.reset_probability = config.get('RESET_PROBABILITY', 0.0)
        self.restart_on_boundaries = config.get('RESTART_ON_BOUNDARIES', False)
        self.initial_value = config.get('INITIAL_VALUE') # Store for potential reset

    def generate_value(self):
        # Check for reset first
        if self.initial_value is not None and random.random() < self.reset_probability:
             self.current_value = self.initial_value
             # Ensure reset value is still within bounds if bounds changed dynamically (unlikely here)
             self.current_value = max(self.min_value, min(self.max_value, self.current_value))

        else: # Perform step change
            step = random.uniform(0, self.max_step)
            increase = random.random() < self.increase_probability

            if increase:
                self.current_value += step
                if self.current_value > self.max_value:
                    if self.restart_on_boundaries and self.initial_value is not None:
                        self.current_value = self.initial_value
                    else:
                        self.current_value = self.max_value
            else:
                self.current_value -= step
                if self.current_value < self.min_value:
                    if self.restart_on_boundaries and self.initial_value is not None:
                        self.current_value = self.initial_value
                    else:
                        self.current_value = self.min_value

        # Return value with correct type
        if self.type == 'int':
            return int(round(self.current_value))
        else:
            # Optional: round float to a certain precision
            return round(float(self.current_value), 4)

class TopicDataBool(TopicDataBase):
     def __init__(self, config):
        super().__init__(config)
        # Start with a random boolean state
        self.current_value = random.choice([True, False])

     def generate_value(self):
         # Simple toggle or could be random based on config
         self.current_value = not self.current_value
         return self.current_value

class TopicDataRawValue(TopicDataBase):
    def __init__(self, config):
        super().__init__(config)
        self.values = config.get('VALUES', [])
        self.restart_on_end = config.get('RESTART_ON_END', False)
        self.value_default = config.get('VALUE_DEFAULT') # Handles defaults for dicts
        self.current_index = 0
        if not self.values:
            print(f"Warning: Raw values list is empty for '{self.name}'. Deactivating.")
            self.is_active = False

    def generate_value(self):
        if not self.is_active or not self.values:
            return None # Nothing to generate

        if self.current_index >= len(self.values):
            if self.restart_on_end:
                self.current_index = 0
            else:
                # Reached end, stop generating for this item
                print(f"Info: Reached end of non-restarting raw values for '{self.name}'. Deactivating.")
                self.is_active = False
                return None

        value = self.values[self.current_index]

        # Handle merging with defaults if the value is a dictionary
        if isinstance(value, dict) and isinstance(self.value_default, dict):
             merged_value = self.value_default.copy()
             merged_value.update(value) # Current value overrides defaults
             value = merged_value
        elif not isinstance(value, dict) and self.value_default is not None:
             # If default is provided but value isn't a dict, maybe just return value?
             # Or log a warning? Behavior depends on expectation.
             pass # Currently just returns the raw value

        self.current_index += 1
        return value

class TopicDataMathExpression(TopicDataBase):
     def __init__(self, config):
        super().__init__(config)
        self.expression = config.get('MATH_EXPRESSION', 'x') # Default to just 'x' if missing
        self.interval_start = config.get('INTERVAL_START', 0.0)
        self.interval_end = config.get('INTERVAL_END', 1.0)
        self.min_delta = config.get('MIN_DELTA', 0.1)
        self.max_delta = config.get('MAX_DELTA', 0.1)
        # Initialize 'x' - use interval_start
        self.x = float(self.interval_start)
        # WARNING: Using eval() is a security risk if the expression comes from untrusted sources.
        # Consider safer alternatives like 'asteval' library if expressions are complex/external.
        print(f"INFO: Math expression for '{self.name}' will use eval(). Ensure expression is safe.")
        # Pre-compile for slight efficiency? Not strictly necessary.
        try:
            self._compiled_expr = compile(self.expression, f"<string>-{self.name}", "eval")
        except Exception as e:
            print(f"ERROR: Could not compile math expression '{self.expression}' for '{self.name}': {e}. Deactivating.")
            self.is_active = False
            self._compiled_expr = None


     def generate_value(self):
         if not self.is_active or self._compiled_expr is None:
             return None

         # Update 'x'
         delta = random.uniform(self.min_delta, self.max_delta)
         self.x += delta
         # Wrap around or reset 'x' if it exceeds the interval
         if self.x > self.interval_end:
             # Simple wrap-around, could also reset to start
             self.x = self.interval_start + (self.x - self.interval_end)
             # Alternative: Reset to start
             # self.x = self.interval_start

         # Evaluate the expression safely (provide math and x)
         try:
             # Provide 'math' module and current 'x' to the expression context
             result = eval(self._compiled_expr, {"math": math}, {"x": self.x})
             # Could be int or float based on expression
             if isinstance(result, (int, float)):
                  return round(result, 4) # Optional rounding
             else:
                  print(f"Warning: Math expression for '{self.name}' did not return a number. Result: {result}")
                  return None
         except Exception as e:
             print(f"ERROR evaluating math expression for '{self.name}' (x={self.x}): {e}")
             # Deactivate on evaluation error to prevent spamming logs
             self.is_active = False
             return None

# ===============================================================
# Topic Thread Class
# ===============================================================

class Topic(threading.Thread):
    def __init__(self,
                 broker_settings: BrokerSettings,
                 topic_url: str,
                 topic_data_config: List[dict], # Use imported List
                 topic_payload_root: Dict,     # Use imported Dict
                 client_settings: ClientSettings):
        threading.Thread.__init__(self, daemon=True) # Daemon threads exit if main program exits

        self.broker_settings = broker_settings
        self.topic_url = topic_url
        self.topic_data = self._load_topic_data(topic_data_config)
        self.topic_payload_root = topic_payload_root or {} # Ensure it's a dict
        self.client_settings = client_settings

        self._stop_event = threading.Event() # For graceful shutdown
        self.client: Optional[mqtt.Client] = None # Use imported Optional

    def _load_topic_data(self, topic_data_configs: List[dict]) -> List[TopicDataBase]: # Use imported List
        """Loads and initializes data generator instances based on config."""
        generators = []
        for config in topic_data_configs:
            data_type = config.get('TYPE', '').lower()
            try:
                if data_type in ['int', 'float']:
                    generators.append(TopicDataNumber(config))
                elif data_type == 'bool':
                    generators.append(TopicDataBool(config))
                elif data_type == 'raw_values':
                    generators.append(TopicDataRawValue(config))
                elif data_type == 'math_expression':
                    generators.append(TopicDataMathExpression(config))
                else:
                    print(f"Warning: Unknown data TYPE '{config.get('TYPE')}' for item '{config.get('NAME', 'N/A')}' in topic '{self.topic_url}'. Skipping.")
            except KeyError as e:
                 print(f"ERROR: Missing required key {e} in data config for item '{config.get('NAME', 'N/A')}' in topic '{self.topic_url}'. Skipping.")
            except Exception as e:
                 print(f"ERROR: Failed to initialize data generator for item '{config.get('NAME', 'N/A')}' in topic '{self.topic_url}': {e}")
        return generators

    def _connect(self) -> bool:
        """Establishes connection to the MQTT broker."""
        if self.client and self.client.is_connected():
             print(f"INFO: Client for {self.topic_url} already connected.")
             return True

        # Create unique client ID, useful for broker logs & non-clean sessions
        # Append random part if clean session is intended, more stable ID otherwise
        base_client_id = f"simulator-{self.topic_url.replace('/', '_')}"
        # Treat None as clean=True for client ID generation
        if self.client_settings.clean is not False:
            client_id = f"{base_client_id}-{random.randint(1000,9999)}"
        else:
            client_id = base_client_id # Requires careful management if multiple instances run!

        # Handle clean_session parameter based on protocol version
        if self.broker_settings.protocol >= mqtt.MQTTv5:
            # MQTTv5 uses clean_start in connect properties, Paho handles it.
            # Do not pass clean_session here. Session expiry is handled by broker/other props.
            self.client = mqtt.Client(client_id=client_id, protocol=self.broker_settings.protocol)
            print(f"INFO: Creating MQTTv5 client for {self.topic_url} (ID: {client_id})")
        else:
            # MQTTv3.1.1 or older: Use clean_session parameter.
            # Default to True if clean setting is None.
            use_clean_session = True if self.client_settings.clean is None else self.client_settings.clean
            self.client = mqtt.Client(client_id=client_id,
                                     protocol=self.broker_settings.protocol,
                                     clean_session=use_clean_session)
            print(f"INFO: Creating MQTTv3 client for {self.topic_url} (ID: {client_id}, CleanSession: {use_clean_session})")


        # Assign callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish # Optional: for logging publish success

        # --- Authentication ---
        if self.broker_settings.username:
            print(f"INFO: Setting username '{self.broker_settings.username}' for {self.topic_url}")
            self.client.username_pw_set(self.broker_settings.username,
                                        self.broker_settings.password) # password can be None if user exists without pw
        else:
             print(f"INFO: No username provided for {self.topic_url}. Connecting anonymously.")


        # --- Attempt Connection ---
        try:
            print(f"INFO: Connecting client for {self.topic_url} to {self.broker_settings.url}:{self.broker_settings.port}...")
            self.client.connect(self.broker_settings.url,
                                self.broker_settings.port,
                                keepalive=60) # Standard keepalive
            self.client.loop_start() # Start network processing thread
            return True # Connection attempt initiated
        except mqtt.WebsocketConnectionError as e:
             print(f"ERROR: Websocket connection failed for {self.topic_url}: {e}")
        except OSError as e: # Socket errors, address in use, etc.
             print(f"ERROR: OS error during connection for {self.topic_url}: {e}")
        except Exception as e: # Catch-all for other potential errors
             print(f"ERROR: Unexpected error connecting client for {self.topic_url}: {e}")

        self.client = None # Ensure client is None if connection failed
        return False

    def disconnect(self):
        """Signals the thread to stop and cleans up the MQTT client."""
        if not self._stop_event.is_set():
             print(f"INFO: Disconnect requested for {self.topic_url}.")
             self._stop_event.set() # Signal run loop to stop

        # Use a temporary variable to avoid race condition if client is set to None elsewhere
        client_instance = self.client
        if client_instance:
            client_was_connected = False
            try:
                # Checking is_connected can sometimes raise exceptions if socket is broken
                client_was_connected = client_instance.is_connected()
            except Exception:
                 pass # Assume not connected if check fails

            try:
                 if client_was_connected:
                     print(f"INFO: Stopping network loop for {self.topic_url}...")
                     client_instance.loop_stop() # Stop background network thread cleanly
                     print(f"INFO: Disconnecting client for {self.topic_url}...")
                     client_instance.disconnect() # Send DISCONNECT packet
                 else:
                     print(f"INFO: Client for {self.topic_url} was not connected, stopping loop anyway.")
                     client_instance.loop_stop() # Ensure loop is stopped even if not connected
            except Exception as e:
                 print(f"WARNING: Exception during disconnect for {self.topic_url}: {e}")
            finally:
                 self.client = None # Clear client instance reference in the thread object
                 if client_was_connected:
                     print(f"INFO: Client for {self.topic_url} disconnected.")


    def run(self):
        """Main thread loop: connect, generate data, publish, wait."""
        print(f"INFO: Starting thread for topic: {self.topic_url}")
        if not self._connect():
            print(f"ERROR: Initial connection failed for {self.topic_url}. Thread exiting.")
            return # Exit thread if initial connection fails

        while not self._stop_event.is_set():
            # Check connection status at the start of each loop iteration
            if not self.client: # If client became None due to disconnect error
                print(f"ERROR: Client for {self.topic_url} is None. Stopping thread.")
                break
            try:
                if not self.client.is_connected():
                    print(f"WARNING: Client for {self.topic_url} detected as disconnected in loop. Stopping thread.")
                    break # Exit loop if connection lost (handled by on_disconnect too, but belt-and-suspenders)
            except Exception as e:
                 print(f"ERROR: Failed to check connection status for {self.topic_url}: {e}. Stopping thread.")
                 break

            try:
                payload_data = self._generate_payload()

                if payload_data is None:
                    print(f"INFO: No active data to publish for {self.topic_url}. Stopping thread.")
                    break # Exit if no data generated

                payload_json = json.dumps(payload_data, indent=None) # Use compact JSON

                # Determine if this specific message should be retained
                # Retain if topic default is True OR any data item requests it via probability
                retain_message = self.client_settings.retain
                if not retain_message: # Only check data items if topic default is False
                    for data_gen in self.topic_data:
                        # Check if generator is active AND asks to retain
                        if data_gen.is_active and hasattr(data_gen, 'should_retain') and data_gen.should_retain():
                            retain_message = True
                            break # One item requesting retain is enough

                # Publish the message
                msg_info = self.client.publish(
                    topic=self.topic_url,
                    payload=payload_json,
                    qos=self.client_settings.qos,
                    retain=retain_message
                )

                # Optional: Wait for publish confirmation (adds latency, useful for QoS 1/2 debugging)
                # try:
                #    msg_info.wait_for_publish(timeout=5.0)
                # except ValueError: # Error if already published?
                #    pass
                # except RuntimeError: # Error if disconnected during wait
                #    print(f"WARNING: Disconnected while waiting for publish confirmation on {self.topic_url}")
                #    break

                # Wait for the next interval, checking stop event periodically
                # This allows quicker shutdown if stop event is set during sleep
                self._stop_event.wait(self.client_settings.time_interval)

            except json.JSONDecodeError as e: # Renamed from JSONDecodeError for clarity
                 print(f"ERROR: Failed to encode payload to JSON for {self.topic_url}: {e}")
                 # Decide whether to continue or stop on encoding errors
                 time.sleep(self.client_settings.time_interval) # Wait before retry/exit
            except mqtt.MQTTException as e:
                 print(f"ERROR: MQTT specific error during publish/loop for {self.topic_url}: {e}")
                 # Often related to connection issues or publishing problems
                 break # Exit loop on MQTT errors
            except AttributeError as e: # Catches potential errors if self.client becomes None unexpectedly
                if "'NoneType' object has no attribute 'publish'" in str(e) or \
                   "'NoneType' object has no attribute 'is_connected'" in str(e):
                    print(f"ERROR: Client object became None unexpectedly for {self.topic_url}. Stopping thread.")
                else:
                    print(f"ERROR: Unexpected AttributeError in run loop for {self.topic_url}: {e}")
                break # Exit loop
            except Exception as e:
                print(f"ERROR: Unexpected error in run loop for {self.topic_url}: {e}")
                # Optional: add more specific error handling
                break # Exit loop on unexpected errors


        # Cleanup after loop exit (graceful or error)
        print(f"INFO: Exiting run loop for {self.topic_url}.")
        self.disconnect() # Ensure client is disconnected cleanly

    def _generate_payload(self) -> Optional[Dict]: # Use imported Optional and Dict
        """Generates the complete payload dictionary for one message."""
        payload: Dict = {} # Initialize payload as a dictionary
        payload.update(self.topic_payload_root) # Start with the static root part

        has_active_generator = False
        active_generators_count = 0
        for data_gen in self.topic_data:
            if data_gen.is_active:
                active_generators_count += 1
                try:
                    value = data_gen.generate_value()
                    # Only add to payload if generator returns a non-None value
                    if value is not None:
                        payload[data_gen.name] = value
                        has_active_generator = True # Mark that we got some dynamic data
                    # else: generator might have finished (e.g., raw values end)
                except Exception as e:
                     # Log error and deactivate the failing generator
                     print(f"ERROR: Failed generating value for '{data_gen.name}' on {self.topic_url}: {e}")
                     data_gen.is_active = False

        # Determine if we should stop the thread:
        # If there are no active generators left AND there's no static root payload,
        # then there's nothing left to send.
        if active_generators_count == 0 and not self.topic_payload_root:
             return None # Signal to stop the thread

        # Return the payload. It might contain only the root, or root + generated data.
        return payload

    # --- MQTT Callbacks ---

    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connection attempt completes."""
        # Map flags dictionary for better logging if needed (MQTTv5)
        connect_flags = flags if isinstance(flags, dict) else {} # Handle V3/V5 differences
        if rc == 0:
            print(f"INFO: Client for {self.topic_url} connected successfully (flags={connect_flags}, rc={rc}).")
            # Optionally: subscribe to topics if this client needs to receive messages
        else:
            error_msg = f"Connection failed for {self.topic_url} with result code {rc}"
            # Use constants for better readability
            if rc == mqtt.CONNACK_REFUSED_PROTOCOL_VERSION: error_msg += " (Incorrect Protocol Version)"
            elif rc == mqtt.CONNACK_REFUSED_IDENTIFIER_REJECTED: error_msg += " (Invalid Client Identifier)"
            elif rc == mqtt.CONNACK_REFUSED_SERVER_UNAVAILABLE: error_msg += " (Server Unavailable)"
            elif rc == mqtt.CONNACK_REFUSED_BAD_USERNAME_PASSWORD: error_msg += " (Bad Username or Password)"
            elif rc == mqtt.CONNACK_REFUSED_NOT_AUTHORIZED: error_msg += " (Not Authorized)"
            # Add more MQTTv5 specific codes if using protocol 5
            elif self.broker_settings.protocol >= mqtt.MQTTv5:
                 # Example V5 codes (check paho-mqtt constants for more)
                 if rc == 128: error_msg += " (Unspecified error - V5)"
                 elif rc == 135: error_msg += " (Not authorized - V5)"
                 # ... other V5 codes
                 else: error_msg += " (Unknown V5 Error)"
            else:
                 error_msg += " (Unknown V3 Error)"

            print(f"ERROR: {error_msg}")
            # Signal the thread to stop if connection is refused (no point retrying in this script)
            self._stop_event.set()

    def _on_disconnect(self, client, userdata, rc):
        """Callback when the client disconnects."""
        if rc == 0:
            # Disconnect called by us (or broker graceful shutdown)
            # This might be logged even during graceful shutdown initiated by stop()
            pass # Reduce noise, already logged in disconnect()
            # print(f"INFO: Client for {self.topic_url} disconnected gracefully (rc=0).")
        else:
            print(f"WARNING: Unexpected disconnection for {self.topic_url} (rc={rc}). Check network or broker logs.")
            # Signal the thread to stop on unexpected disconnects
            self._stop_event.set()

    def _on_publish(self, client, userdata, mid):
        """Callback when a message is successfully published (for QoS > 0)."""
        # Keep this minimal to avoid console spam
        # print(f'DEBUG: MID {mid} published on {self.topic_url}')
        pass

# --- END OF FILE topic.py ---
