from datetime import datetime, timedelta

from pydantic import BaseModel, NaiveDatetime, Field


class Token(BaseModel):
    token: str
    expires_in_seconds: int = Field(default=86400)
    created_at: NaiveDatetime = Field(default_factory=datetime.now)
    token_type: str = "Bearer"

    @property
    def expires_at(self) -> NaiveDatetime:
        return self.created_at + timedelta(seconds=self.expires_in_seconds)

    @property
    def expired(self) -> bool:
        return self.expires_at <= datetime.now()


class TekionCredentials(BaseModel):
    auth_uri: str
    access_key: str
    secret_key: str
    client_id: str

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
