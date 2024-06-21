import logging
from csv import reader
from datetime import datetime

from domain.accelerometer import Accelerometer
from domain.aggregated_data import AggregatedData
from domain.gps import Gps
from domain.parking import Parking

logger = logging.getLogger(__name__)


class FileDataSource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_reader = None
        self.gps_reader = None
        self.parking_reader = None
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    @classmethod
    def _parse_accelerometer_data(cls, accelerometer_data: list) -> Accelerometer | None:
        if not accelerometer_data:
            return None

        return Accelerometer(
            x=accelerometer_data[0],
            y=accelerometer_data[1],
            z=accelerometer_data[2],
        )

    @classmethod
    def _parse_gps_data(cls, gps_data: list) -> Gps | None:
        if not gps_data:
            return None

        return Gps(
            longitude=gps_data[0],
            latitude=gps_data[1],
        )

    def _parse_parking_data(self, parking_data: list) -> Parking | None:
        if not parking_data:
            return None

        return Parking(
            empty_count=parking_data[0],
            gps=self._parse_gps_data(parking_data[1:]),
        )

    def read(self) -> AggregatedData:
        if not self.accelerometer_reader and not self.gps_reader and not self.parking_reader:
            raise Exception("Reading has not been started")

        accelerometer_data = None
        gps_data = None
        parking_data = None

        try:
            accelerometer_data = next(self.accelerometer_reader)
        except StopIteration:
            self.restart_accelerometer_reading()

        try:
            gps_data = next(self.gps_reader)
        except StopIteration:
            self.restart_gps_reading()

        try:
            parking_data = next(self.parking_reader)
        except StopIteration:
            self.restart_parking_reading()

        return AggregatedData(
            accelerometer=self._parse_accelerometer_data(accelerometer_data),
            gps=self._parse_gps_data(gps_data),
            # parking=self._parse_parking_data(parking_data),
            timestamp=datetime.now()
        )

    def _start_reading_accelerometer_data(self) -> None:
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        self.accelerometer_reader = reader(self.accelerometer_file, delimiter=',')
        try:
            next(self.accelerometer_reader)
        except StopIteration:
            raise Exception(f"{self.accelerometer_filename} is empty")
        logger.info("Accelerometer data reading restarted")

    def _start_reading_parking_data(self) -> None:
        self.parking_file = open(self.parking_filename, 'r')
        self.parking_reader = reader(self.parking_file, delimiter=',')
        try:
            next(self.parking_reader)
        except StopIteration:
            raise Exception(f"{self.parking_file} is empty")
        logger.info("Parking data reading restarted")

    def _start_reading_gps_data(self) -> None:
        self.gps_file = open(self.gps_filename, 'r')
        self.gps_reader = reader(self.gps_file, delimiter=',')
        try:
            next(self.gps_file)
        except StopIteration:
            raise Exception(f"{self.gps_filename} is empty")
        logger.info("GPS data reading restarted")

    def start_reading(self, *args, **kwargs) -> None:
        self._start_reading_accelerometer_data()
        self._start_reading_gps_data()
        self._start_reading_parking_data()

    def _stop_reading_accelerometer_data(self):
        if self.accelerometer_file:
            self.accelerometer_file.close()
            self.accelerometer_file = None

        self.accelerometer_reader = None
        logger.info("Accelerometer data reading stopper")

    def _stop_reading_gps_data(self):
        if self.gps_file:
            self.gps_file.close()
            self.gps_file = None

        self.gps_reader = None
        logger.info("GPS data reading stopped")

    def _stop_reading_parking_data(self):
        if self.parking_file:
            self.parking_file.close()
            self.parking_file = None

        self.parking_reader = None
        logger.info("Parking data reading stopper")

    def stop_reading(self, *args, **kwargs):
        self._stop_reading_accelerometer_data()
        self._stop_reading_gps_data()
        self._stop_reading_parking_data()
        logger.info("Data reading stopped")

    def restart_accelerometer_reading(self):
        self._stop_reading_accelerometer_data()
        self._start_reading_accelerometer_data()
        logger.info("Accelerometer data reading restarted")

    def restart_gps_reading(self):
        self._stop_reading_gps_data()
        self._start_reading_gps_data()
        logger.info("GPS data reading restarted")

    def restart_parking_reading(self):
        self._stop_reading_parking_data()
        self._start_reading_parking_data()
        logger.info("Parking data reading restarted")
