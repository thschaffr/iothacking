# --- START OF FILE data_classes/client_settings.py ---

from dataclasses import dataclass
from typing import Optional

@dataclass
class ClientSettings:
    # clean: bool # Original from user file
    clean: Optional[bool] # <-- MODIFIED: Make optional for flexibility
    retain: bool
    qos: int
    time_interval: int

# --- END OF FILE data_classes/client_settings.py ---
