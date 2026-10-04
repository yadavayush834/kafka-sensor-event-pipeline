"""Consume, validate, store, then commit each Kafka offset."""

import logging
import os
import signal

from confluent_kafka import Consumer, KafkaException

from .events import decode_reading
from .storage import connect, save_reading


LOG = logging.getLogger(__name__)


def process_message(connection, message) -> bool:
    reading = decode_reading(message.value())
    if message.key() != reading.sensor_id.encode("utf-8"):
        raise ValueError("Kafka key does not match sensor_id")
    return save_reading(connection, reading, message.topic(), message.partition(), message.offset())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    topic = os.getenv("KAFKA_TOPIC", "sensor-readings")
    connection = connect(os.getenv("DATABASE_PATH", "readings.db"))
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "sensor-storage-v1"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    LOG.info("consuming topic=%s", topic)
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            inserted = process_message(connection, message)
            # A failed commit can replay the record; the SQLite key makes replay safe.
            consumer.commit(message=message, asynchronous=False)
            LOG.info("%s partition=%d offset=%d", "stored" if inserted else "duplicate", message.partition(), message.offset())
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
