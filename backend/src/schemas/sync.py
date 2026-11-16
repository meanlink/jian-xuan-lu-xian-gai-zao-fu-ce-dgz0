from pydantic import BaseModel, Field


class StationInput(BaseModel):
    code: str = Field(min_length=2, max_length=80)
    name: str = Field(min_length=2, max_length=160)
    endpoint: str = Field(default='local://source-simulator', max_length=240)
    cursor_ttl_seconds: int = Field(default=3600, ge=60, le=30 * 86400)


class SourceEventInput(BaseModel):
    op: str = Field(pattern='^(upsert|withdraw)$')
    record_key: str = Field(min_length=1, max_length=160)
    payload: dict = Field(default_factory=dict)


class CursorAdvanceInput(BaseModel):
    retain_after_version: int = Field(ge=0)
