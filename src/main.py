import logging
import time

from paho.mqtt import client as mqtt_client

import config
from file_datasource import FileDataSource
from schema.aggregated_data_schema import AggregatedDataSchema

logger = logging.getLogger(__name__)


def connect_mqtt(broker, port):
    """Create MQTT client"""
    logger.info(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            logger.info(f"Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, topic, datasource, delay):
    datasource.start_reading()
    while True:
        time.sleep(delay)
        try:
            data = datasource.read()
        except Exception:
            break
        msg = AggregatedDataSchema().dumps(data)
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            # logger.info(f"Send `{msg}` to topic `{topic}`")
            pass
        else:
            logger.info(f"Failed to send message to topic {topic}")


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDataSource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, config.MQTT_TOPIC, datasource, config.DELAY)


if __name__ == '__main__':
    run()
