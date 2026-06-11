import json
import os

def load_vault(platform_name: str) -> dict:
    """Reads the JSON database from disk and returns a dictionary indexed by job ID."""
    # Defensive check: ensure the storage directory exists up front
    os.makedirs("backend/vaults", exist_ok=True)
    
    db_file = f"backend/vaults/{platform_name.lower()}_vault.json"
    if not os.path.exists(db_file):
        return {}
    try:
        with open(db_file, "r", encoding="utf-8") as f:
            records = json.load(f)
            vault = {}
            for job in records if isinstance(records, list) else []:
                job_id = job.get("id") if isinstance(job, dict) else None
                if job_id:
                    vault[job_id] = job
            return vault
    except (json.JSONDecodeError, OSError) as e:
         print(f"[Memory Error] Failed to read database, starting fresh: {e}")
         return {}

def save_vault(vault_data: dict, platform_name: str):
    """Serializes the memory dictionary into a clean list format and saves it to disk."""
    # Defensive check: make sure the directory is there before writing files
    os.makedirs("backend/vaults", exist_ok=True)
    
    db_file = f"backend/vaults/{platform_name.lower()}_vault.json"
    tmp_file = f"{db_file}.tmp"
    try:
        records_list = list(vault_data.values())
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(records_list, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, db_file)
        print(f"[Memory] Successfully wrote {len(records_list)} total records to '{db_file}'.")
    except OSError as e:
        print(f"[Memory Error] Failed to save database to disk: {e}")
        raise

def is_duplicate(job_id: str, vault_data: dict) -> bool:
    """Returns True if the job has already been discovered or processed."""
    return job_id in vault_data