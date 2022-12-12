from dataclasses import dataclass
from dataclasses_json import dataclass_json, Undefined


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class DBConnectionConfig:
    user: str
    password: str
    host: str
    port: str
    db: str
