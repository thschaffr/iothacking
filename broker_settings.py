# --- START OF FILE data_classes/broker_settings.py ---

from dataclasses import dataclass
from typing import Optional

@dataclass
class BrokerSettings:
    url: str
    port: int
    protocol: int
    username: Optional[str] = None # <-- ADDED
    password: Optional[str] = None # <-- ADDED

# --- END OF FILE data_classes/broker_settings.py ---
