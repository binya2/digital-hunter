import uuid
from enum import Enum

from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class SignalType(str, Enum):
    SIGINT = "SIGINT"
    VISING = "VISINT"
    HUMINT = "HUMINT"


class WeaponType(str, Enum):
    AGM_114_Hellfire = "AGM-114 Hellfire"
    GBU_39_SDB = "GBU-39 SDB"
    Delilah_Missile = "Delilah Missile"
    SPICE_250 = "SPICE-250"
    Popeye_AGM = "Popeye AGM"
    Griffin_LGM = "Griffin LGM"


class DamageResult(str, Enum):
    destroyed = "destroyed"
    damaged = "damaged"
    no_damage = "no_damage"


class IntelEvent(BaseModel):
    timestamp: datetime
    signal_id: uuid.UUID
    entity_id: str = Field(..., min_length=7, max_length=7)
    reported_lat: float = Field(..., ge=-90, le=90)
    reported_lon: float = Field(..., ge=-180, le=180)
    signal_type: SignalType
    priority_level: int

    @field_validator('priority_level')
    @classmethod
    def validate_priority(cls, v):
        if v not in [1, 2, 3, 4, 5, 99] and v is not None:
            raise ValueError("Priority must be between 1-5 or 99")
        return v


class AttackEvent(BaseModel):
    timestamp: datetime
    attack_id: uuid.UUID
    entity_id: str = Field(..., min_length=7, max_length=7)
    weapon_type: WeaponType


class DamageEvent(BaseModel):
    timestamp: datetime
    attack_id: uuid.UUID
    entity_id: str = Field(..., min_length=7, max_length=7)
    result: DamageResult
