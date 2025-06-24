from dagster import Definitions, load_assets_from_modules
from .resources import *
from .assets import *


all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=[*all_assets],
    resources={'dev_s3': s3_resource},
    jobs=[],
    schedules=[],
    sensors=[],
)