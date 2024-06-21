from dataclasses import dataclass
from datetime import datetime

from domain.accelerometer import Accelerometer
from domain.parking import Parking
from domain.gps import Gps


@dataclass
class AggregatedData:
    accelerometer: Accelerometer
    gps: Gps
    # parking: Parking
    timestamp: datetime
