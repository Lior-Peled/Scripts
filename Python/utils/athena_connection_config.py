from dataclasses import dataclass
from dataclasses_json import dataclass_json, Undefined


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class AthenaConnectionConfig:
    s3_staging_dir: str
    region_name: str
    access_key: str
    secret_key: str
    schema: str
