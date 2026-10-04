"""Generate mock readings and publish them with sensor ID as the Kafka key."""

import argparse
import os
import random
import sys
import time

from confluent_kafka import Producer

from .events import make_reading


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish mock sensor events")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--interval", type=float, default=0.2, help="seconds between events")
    parser.add_argument("--sensors", type=int, default=3)
    args = parser.parse_args()
    if args.count < 1 or args.interval < 0 or args.sensors < 1:
        parser.error("count and sensors must be positive; interval cannot be negative")

    topic = os.getenv("KAFKA_TOPIC", "sensor-readings")
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "enable.idempotence": True,
        "acks": "all",
    })
    failed = []

    def delivery_report(error, message):
        if error is not None:
            failed.append(str(error))
        else:
            print(f"sent sensor={message.key().decode()} partition={message.partition()} offset={message.offset()}")

    for index in range(args.count):
        sensor_id = f"sensor-{index % args.sensors + 1:02d}"
        reading = make_reading(
            sensor_id,
            round(random.uniform(18, 42), 1),
            round(random.uniform(30, 90), 1),
        )
        # poll serves delivery callbacks and keeps the local queue moving.
        producer.poll(0)
        producer.produce(topic, key=sensor_id.encode(), value=reading.to_bytes(), on_delivery=delivery_report)
        time.sleep(args.interval)

    remaining = producer.flush(30)
    if remaining or failed:
        print(f"delivery failed: {remaining} queued, {len(failed)} errors {failed[:3]}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
