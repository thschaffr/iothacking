# --- START OF FILE main.py ---

import argparse
import sys
from pathlib import Path
from simulator import Simulator # Import the updated Simulator

# --- Helper Functions ---
def get_project_root() -> Path:
    """Gets the root directory of the project (assuming main.py is in the root)."""
    return Path(__file__).resolve().parent

def default_settings_path() -> Path:
    """Provides the default path for the settings file."""
    return get_project_root() / 'config' / 'settings.json'

def is_valid_file(parser: argparse.ArgumentParser, arg: str) -> Path:
    """Checks if the argument is a valid file path."""
    settings_path = Path(arg)
    if settings_path.is_file():
        return settings_path
    else:
        # Try resolving relative to the project root if the direct path fails
        alt_path = get_project_root() / arg
        if alt_path.is_file():
            return alt_path
        else:
            parser.error(f"Cannot open settings file: '{arg}' or '{alt_path}'")
            # The parser.error call exits the script, so no return is strictly needed after it.
            # Added for clarity in case error behavior changes.
            sys.exit(1)


# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MQTT Simulator: Publishes simulated data based on a JSON configuration.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-f', '--file',
        dest='settings_file',
        type=lambda x: is_valid_file(parser, x),
        help='Path to the settings JSON file.',
        default=default_settings_path()
    )
    args = parser.parse_args()

    print("========================================")
    print("          MQTT Data Simulator           ")
    print("========================================")
    print(f"Using settings file: {args.settings_file.resolve()}")

    # Create and run the simulator
    try:
        simulator = Simulator(args.settings_file)
        simulator.run() # This now blocks until Ctrl+C or all threads finish/error
    except Exception as e:
         print(f"\nFATAL ERROR: Simulator failed unexpectedly: {e}")
         sys.exit(1)

    print("========================================")
    print("         Simulator Finished            ")
    print("========================================")
    sys.exit(0) # Explicit successful exit

# --- END OF FILE main.py ---
