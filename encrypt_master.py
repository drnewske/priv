import json
import base64
import hashlib
import secrets
import hmac
import struct
import time
import logging
import os
from typing import Dict, Any, Optional
import requests

# --- CONFIGURATION ---
# This MUST point to the same remote config.json Gist that your other script uses.
# It's needed to get the salt, app_identifier, etc., to ensure keys match.
CONFIG_URL = "https://gist.githubusercontent.com/drnewske/6070fc714b3e86e493e3d9fc87738459/raw/fe61be1780db177817ba8412fb7490bc28a95123/config.json"

# The permanent, fixed filename for the output.
OUTPUT_FILENAME = "master_config.json"
LOG_FILE = "master_encryptor.log"

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CORE ENCRYPTOR CLASS (Identical to the other script for key consistency) ---

class MasterEncryptor:
    def __init__(self):
        """Initializes the encryptor."""
        self.config: Optional[Dict[str, Any]] = None
        self.output_file = OUTPUT_FILENAME

    def fetch_remote_config(self) -> bool:
        """Fetches the remote configuration to get salt and other crypto parameters."""
        logger.info(f"Fetching remote configuration from {CONFIG_URL} to ensure key consistency...")
        try:
            response = requests.get(CONFIG_URL, timeout=15)
            response.raise_for_status()
            self.config = response.json()
            logger.info("✅ Remote configuration loaded successfully.")
            return True
        except Exception as e:
            logger.error(f"❌ FAILED to fetch remote config: {e}", exc_info=True)
            return False

    def generate_deterministic_key(self, seed: str, salt: bytes, purpose: str) -> bytes:
        """Generates a deterministic key using PBKDF2-HMAC-SHA256."""
        combined = f"{seed}:{self.config['app_identifier']}:{self.config['version']}:{purpose}".encode('utf-8')
        dk = hashlib.pbkdf2_hmac('sha256', combined, salt, self.config['key_iterations'])
        return dk[:32]

    def stream_encrypt(self, data: bytes, key: bytes, iv: bytes) -> bytes:
        """Encrypts data using a stream cipher approach."""
        result = bytearray()
        key_hash = hashlib.sha256(key + iv).digest()
        for i, byte in enumerate(data):
            pos_key = hashlib.sha256(key_hash + struct.pack('<I', i)).digest()[0]
            result.append(byte ^ pos_key)
        return bytes(result)

    def create_hmac(self, data: bytes, key: bytes) -> bytes:
        """Creates an HMAC-SHA256 tag for authentication."""
        return hmac.new(key, data, hashlib.sha256).digest()

    def encrypt_payload(self, data: Dict[Any, Any]) -> Optional[Dict[str, Any]]:
        """Encrypts the given data payload using the loaded configuration."""
        logger.info("Starting encryption process for Master JSON...")
        try:
            json_str = json.dumps(data, separators=(',', ':'))
            json_bytes = json_str.encode('utf-8')

            app_salt = self.config['app_salt'].encode('utf-8')
            master_seed = f"{self.config['app_identifier']}:{self.config['version']}"

           # Add a "master_" prefix to generate a unique set of keys
            key1 = self.generate_deterministic_key(master_seed, app_salt, "master_layer1")
            key2 = self.generate_deterministic_key(master_seed, app_salt, "master_layer2")
            hmac_key = self.generate_deterministic_key(master_seed, app_salt, "master_hmac")

            iv = secrets.token_bytes(16)
            timestamp = struct.pack('<Q', int(time.time()))
            data_with_timestamp = timestamp + json_bytes

            encrypted_layer1 = self.stream_encrypt(data_with_timestamp, key1, iv)
            encrypted_layer2 = self.stream_encrypt(encrypted_layer1, key2, iv)

            message_to_auth = iv + encrypted_layer2
            auth_tag = self.create_hmac(message_to_auth, hmac_key)

            final_payload = iv + encrypted_layer2 + auth_tag
            encrypted_string = base64.b64encode(final_payload).decode('ascii')

            logger.info("✅ Master JSON encryption successful.")
            return {
                "encrypted_data": encrypted_string,
                "timestamp": int(time.time()),
                "status": "success",
            }
        except Exception as e:
            logger.error(f"❌ FAILED during encryption: {e}", exc_info=True)
            return None

    def save_encrypted_data(self, encrypted_result: Dict[str, Any]):
        """Saves the final encrypted blob to the output file."""
        logger.info(f"Saving encrypted data to '{self.output_file}'...")
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(encrypted_result, f, indent=2)
            logger.info(f"✅ Data saved successfully.")
        except Exception as e:
            logger.error(f"❌ FAILED to save encrypted data: {e}", exc_info=True)

def main():
    """Main function to run the master encryption service."""
    logger.info("🚀 Starting Master JSON Encryption Run")
    logger.info("="*60)

    # --- HARDCODED MASTER JSON DATA ---
    # If you need to make changes, edit them here and rerun the workflow.
    master_data = {
      "master_url": {
        "our_site_url": "https://groinfont.com/b45tvhb0?key=b59073054215fe97c24a1dd31c58aecb",
        "telegram_link": "https://t.me/+37u6NHt-LN8wMjlk",
        "whatsapp_link": "https://www.whatsapp.com/channel/0029Va1fS95EAKWGIf0u5O1W",
        "events_card_json_link": "https://raw.githubusercontent.com/drnewske/GDDNvsndjhqwh353dmjje-nnnswwwwwwwwwwwwwwwww5rwtqsmmvb/refs/heads/main/67d18f5b263505d3be8283897bb383f149a39dd35bf9563d43.json",
        "full_matches_replay_json_link": "https://cdn.jsdelivr.net/gh/drnewske/reAepo1no3489repo34xserdQrtmmfbhdaej/matches.json",
        "tv_channels": "https://raw.githubusercontent.com/drnewske/priv/refs/heads/main/tv.json",
        "important_messages": {
          "urgent": {
            "tag": "null",
            "message": "null",
            "update_link": "null"
          },
          "temporary": {
            "id": "promo_july2025",
            "message": "THE APP WORKS FINE, PERFECTO!!. This is where is you will advertise your products! for free?...get serious comrade",
            "dismissible": False
          }
        }
      }
    }

    encryptor = MasterEncryptor()

    # The process: Fetch config -> Encrypt hardcoded data -> Save file
    if encryptor.fetch_remote_config():
        encrypted_result = encryptor.encrypt_payload(master_data)
        if encrypted_result:
            encryptor.save_encrypted_data(encrypted_result)

    logger.info("🏁 Master JSON Encryption Run Finished")
    logger.info("="*60 + "\n")

if __name__ == "__main__":
    main()
