# --- START OF FILE utils.py ---

import random

def should_run_with_probability(probability: float):
    """Returns True with a given probability."""
    if not 0.0 <= probability <= 1.0:
        raise ValueError("Probability must be between 0.0 and 1.0")
    return random.random() < probability

# --- END OF FILE utils.py ---
