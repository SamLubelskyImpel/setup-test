import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urljoin

from .envs import CRM_TEKION_AUTH_ENDPOINT


@dataclass
class Token:
    token: str
    expires_in_seconds: int | None = field(default=86400)
    created_at: datetime | None = field(default_factory=datetime.now)
    token_type: str | None = field(default="Bearer")

    @property
    def expires_at(self) -> datetime:
        return self.created_at + timedelta(seconds=self.expires_in_seconds)

    @property
    def expired(self) -> bool:
        return self.expires_at <= datetime.now()

    def as_dict(self) -> dict:
        return {
            "token": self.token,
            "expires_in_seconds": self.expires_in_seconds,
            "created_at": self.created_at.isoformat(),
            "token_type": self.token_type,
        }

    def as_json(self) -> str:
        return json.dumps(self.as_dict())


@dataclass
class TekionCredentials:
    url: str
    access_key: str
    secret_key: str
    client_id: str

    @property
    def auth_uri(self) -> str:
        return urljoin(self.url, CRM_TEKION_AUTH_ENDPOINT)

    @property
    def headers(self) -> dict[str, str]:
        headers = {
            "accept": "application/json",
            "client_id": self.client_id,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        return headers

    @property
    def data(self) -> dict[str, str]:
        data = {
            "access-key": self.access_key,
            "secret-key": self.secret_key
        }
        return data

    def as_dict(self) -> dict:
        return {
            "url": self.url,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "client_id": self.client_id,
        }

    def as_json(self) -> str:
        return json.dumps(self.as_dict())
