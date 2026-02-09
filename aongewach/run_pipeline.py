import os
import subprocess
import sys

def run_step(script_name, description):
    print(f"\n{'='*50}")
    print(f"STEP: {description}")
    print(f"Running {script_name}...")
    print(f"{'='*50}\n")
    
    try:
        # Run unbuffered output
        result = subprocess.run([sys.executable, script_name], check=True)
        if result.returncode == 0:
            print(f"\n[SUCCESS] {script_name} completed.")
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] {script_name} failed with return code {e.returncode}.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Failed to run {script_name}: {e}")
        sys.exit(1)

def main():
    # 1. Scrape Schedule (Weekly)
    run_step("scrape_schedule.py", "Scraping Weekly Schedule from Wheresthematch.com")
    
    # 2. Map Teams (Event Names -> Team IDs & Logos)
    run_step("map_schedule_to_teams.py", "Mapping Events to Team IDs and Logos")
    
    # 3. Map Channels (Schedule Channels -> IPTV Stream URLs)
    run_step("map_channels.py", "Mapping Schedule Channels to Playable IPTV Streams")

    print(f"\n{'='*50}")
    print("PIPELINE COMPLETE")
    print("Output available in: weekly_schedule_final.json")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
