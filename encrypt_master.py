import json
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import os

# --- CONFIGURATION ---

# THIS IS THE ONE AND ONLY KEY. IT MUST MATCH EXACTLY THE ONE IN YOUR WORKER'S SECRET.
# Key: 5H6nSgP+nARaK4a434s8J7+AYYp7Unz9jV4A+A7d8vA=
# This is the Base64 representation. We will decode it for use.
DECRYPTION_KEY_B64 = "5H6nSgP+nARaK4a434s8J7+AYYp7Unz9jV4A+A7d8vA="

# --- IMPORTANT: DEFINE YOUR MASTER CONFIG DATA HERE ---
# Add your decryption_worker_url which points to your Cloudflare worker.
master_config_data = {
  "master_url": {
    "our_site_url": "https://orstreams.info",
    "telegram_link": "https://t.me/+37u6NHt-LN8wMjlk",
    "whatsapp_link": "https://www.whatsapp.com/channel/0029Va1fS95EAKWGIf0u5O1W",
    "events_card_json_link": "https://gddnvsndjhqwh353dmjje-nnnswwwwwwwwwwwwwwwww5rwtqsmmvbjumaj.pages.dev/live_events.json",
    # This URL is NOT for decryption anymore. It's for fetching the KEY.
    "key_server_url": "https://or-streams-decryptor.ashamedke.workers.dev/",
    "full_matches_replay_json_link": "https://cdn.jsdelivr.net/gh/drnewske/reAepo1no3489repo34xserdQrtmmfbhdaej/matches.json",
    "tv_channels": "https://cdn.jsdelivr.net/gh/drnewske/priv@main/tv.json",
    "predictions": "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/today_matches.json",
    "league_tables": "https://cdn.jsdelivr.net/gh/drnewske/priv@main/standings.json",
    "live_scores": "https://cdn.jsdelivr.net/gh/drnewske/priv@main/lvscore.json",
    "tv_chnnels_back_up": "https://cdn.jsdelivr.net/gh/drnewske/priv@main/ads.json",
    "mostrosity": "https://cdn.jsdelivr.net/gh/drnewske/priv@main/ads.json",
    "important_messages": {
        "urgent": {"tag": "null", "message": "null", "update_link": "null"},
        "temporary": {"id": "null", "message": "null", "dismissible": "null"}
    },
    "blocked_urls": []
  }
}

def encrypt_data(data_to_encrypt: dict, key: bytes) -> str:
    """Encrypts a dictionary using AES-GCM and returns a Base64 string."""
    
    # Convert the dictionary to a JSON string, then to bytes
    plaintext_bytes = json.dumps(data_to_encrypt).encode('utf-8')
    
    # Set up the AES-GCM cipher
    cipher = AES.new(key, AES.MODE_GCM)
    
    # Encrypt the data
    ciphertext, tag = cipher.encrypt_and_digest(plaintext_bytes)
    
    # The nonce, tag, and ciphertext must be combined to be decrypted.
    # The structure will be: [12-byte nonce][16-byte tag][ciphertext]
    # This is a standard and secure way to package the data.
    payload_to_encode = cipher.nonce + tag + ciphertext
    
    # Return the result as a Base64 encoded string
    return base64.b64encode(payload_to_encode).decode('utf-8')

def main():
    """Main function to encrypt the master config and wrap it."""
    print("🚀 Starting secure encryption run...")
    
    # Decode the Base64 key to get the raw bytes for encryption
    key_bytes = base64.b64decode(DECRYPTION_KEY_B64)

    # Encrypt the master configuration data
    encrypted_master_config = encrypt_data(master_config_data, key_bytes)
    
    # The final output file should contain ONLY the encrypted data blob,
    # just like your previous setup.
    output_blob = {
        "encrypted_data": encrypted_master_config
    }
    
    output_filename = "master_config.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(output_blob, f, indent=2)
        
    print(f"✅ Successfully encrypted and saved to '{output_filename}'")
    print("🏁 Encryption run finished.")

if __name__ == "__main__":
    main()
