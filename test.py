import json
import time
import threading
import paho.mqtt.client as mqtt


class Topic(threading.Thread):
    def __init__(self, topic_url, broker_settings, client_settings, topic_data, topic_payload_root):
        super().__init__()  # required for threading
        self.topic_url = topic_url
        self.broker_settings = broker_settings
        self.client_settings = client_settings
        self.topic_data = topic_data
        self.topic_payload_root = topic_payload_root
        self.payload = None
        self.loop = False

    def connect(self):
        self.loop = True
        clean_session = None if self.broker_settings.protocol == mqtt.MQTTv5 else self.client_settings.clean

        self.client = mqtt.Client(self.topic_url, protocol=self.broker_settings.protocol, clean_session=clean_session)
        self.client.on_publish = self.on_publish

        # 🔧 Support both lowercase and UPPERCASE keys
        username = getattr(self.broker_settings, 'username', None) or getattr(self.broker_settings, 'BROKER_USERNAME', None)
        password = getattr(self.broker_settings, 'password', None) or getattr(self.broker_settings, 'BROKER_PASSWORD', None)

        if username:
            print(f"[DEBUG] Using MQTT auth → username: {username}")
            self.client.username_pw_set(username, password)
        else:
            print("[DEBUG] No MQTT auth being used")

        print(f"[DEBUG] Connecting to {self.broker_settings.url}:{self.broker_settings.port} ...")
        self.client.connect(self.broker_settings.url, self.broker_settings.port)
        self.client.loop_start()

    def disconnect(self):
        self.loop = False
        self.client.loop_stop()
        self.client.disconnect()

    def run(self):
        self.connect()
        while self.loop:
            self.payload = self.generate_payload()
            self.client.publish(
                topic=self.topic_url,
                payload=json.dumps(self.payload),
                qos=self.client_settings.qos,
                retain=self.client_settings.retain
            )
            time.sleep(self.client_settings.time_interval)

    def on_publish(self, client, userdata, result):
        print(f"[{time.strftime('%H:%M:%S')}] Data published on: {self.topic_url}")

    def generate_payload(self):
        payload = {}
        payload.update(self.topic_payload_root)

        has_data_active = False
        for data in self.topic_data:
            if data.is_active:
                has_data_active = True
                payload[data.name] = data.generate_value()

        if not has_data_active:
            self.disconnect()
            return

        return payload
