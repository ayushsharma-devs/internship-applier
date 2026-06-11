# applier.py
import json
import os
from playwright.async_api import Page
import time
class ApplicationPipeline:
    def __init__(self, page: Page, profile_data: dict):
        self.page = page
        self.profile_data = profile_data
        self.vault_root = "backend/vaults"

    async def run_pipeline(self, url: str, adapter: any) -> str:
        """Coordinates the application transaction via the selected platform adapter."""
        # Cleanly load specific selectors onto the adapter dynamically
        loaded = self._load_platform_selectors(adapter.platform_key)
        if loaded:
           adapter.selectors = loaded
        elif not getattr(adapter, "selectors", None):
            raise ValueError(f"No selectors configured for platform '{adapter.platform_key}'")
        
        # Delegate the actual heavy-lifting to the specialized platform code
        execution_status = await adapter.apply(self.page, url, self.profile_data)
        
        # Log transactions to memory vaults exactly like your legacy framework did
        self._write_to_vault(url, execution_status, adapter.platform_key)
        
        return execution_status

    def _load_platform_selectors(self, platform_key: str) -> dict:
        # Mock or pull from your centralized config JSON paths
        return {} 

    def _write_to_vault(self, url: str, status: str, platform: str):
        """Maintains the local JSON audit records exactly like your legacy setup."""
        os.makedirs(self.vault_root, exist_ok=True)
        vault_path = os.path.join(self.vault_root, f"{platform.lower()}_vault.json")
        if not os.path.exists(vault_path):
            records = []
        else:
            with open(vault_path, 'r', encoding="utf-8") as f:
                try:
                    records = json.load(f)
                except json.JSONDecodeError:
                    records = []

        records.append({
            "url": url,
            "status": status,
            "platform": platform,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) # Or your live datetime string
        })

        with open(vault_path, 'w', encoding="utf-8") as f:
            json.dump(records, f, indent=4)
        print(f"[Memory] Successfully wrote {len(records)} total records to '{vault_path}'.")