import os
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx

from src.constants import STORAGE_DIR

_SALES_FORCE_API_BASE_URL = os.environ["SALES_FORCE_API_BASE_URL"]
_SALES_FORCE_CLIENT_ID = os.environ["SALES_FORCE_CLIENT_ID"]
_SALES_FORCE_SECRET = os.environ["SALES_FORCE_SECRET"]


def _persist_content_as_file(asset_id: int, asset_name: str, content: str) -> None:
    with open(f"{STORAGE_DIR}/{asset_name}-{asset_id}", "w") as f:
        f.write(content)


@dataclass
class SalesForceAPI:
    access_token: str
    expires_at: datetime
    rest_instance_url: str
    retrieved_ids = set()

    def __init__(self):
        self._authenticate_salesforce()

    def _authenticate_salesforce(self) -> None:
        res = httpx.post(f"{_SALES_FORCE_API_BASE_URL}/v2/token",
             headers={
               "Content-Type": "application/json"
             },
             data={
                "grant_type": "client_credentials",
                "client_id": _SALES_FORCE_CLIENT_ID,
                "client_secret": _SALES_FORCE_SECRET,
                "scope": "email_read email_write email_send",
                "account_id": "12345"
            }
        )

        if res.status_code == 200:
            json = res.json()
            self.access_token = json["access_token"]
            self.expires_at = datetime.now() + timedelta(seconds=json["expires_in"])
            self.rest_instance_url = json["rest_instance_url"]

    def _fetch_objects_per_page(self, page: int) -> None:
        if self.expires_at > datetime.now():
            self._authenticate_salesforce()

        res = httpx.post(f"{self.rest_instance_url}/asset/v1/content/assets/query?order=desc")
        json = res.json()

        for item in json["items"]:
            if not item["id"] in self.retrieved_ids:
                self.retrieved_ids.add(item["id"])
                _persist_content_as_file(
                    asset_id=item["id"],
                    asset_name=item["name"],
                    content=item["content"],
                )

        if json["pageSize"] >= 50:
            self._fetch_objects_per_page(page + 1)

    def fetch_binaries_from_sales_force(self):
        self._fetch_objects_per_page(1)


sales_force_api = SalesForceAPI()
