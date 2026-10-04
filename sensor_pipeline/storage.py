"""SQLite sink. Each insert and its deduplication check are one transaction."""

import sqlite3
from pathlib import Path

from .events import SensorReading, classify


def connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute(
        """CREATE TABLE IF NOT EXISTS readings (
            event_id TEXT PRIMARY KEY,
            sensor_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            temperature_c REAL NOT NULL,
            humidity_pct REAL NOT NULL,
            status TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            offset_id INTEGER NOT NULL,
            UNIQUE(topic, partition_id, offset_id)
        )"""
    )
    return connection


def save_reading(
    connection: sqlite3.Connection,
    reading: SensorReading,
    topic: str,
    partition: int,
    offset: int,
) -> bool:
    with connection:
        cursor = connection.execute(
            """INSERT OR IGNORE INTO readings
            (event_id, sensor_id, observed_at, temperature_c, humidity_pct,
             status, topic, partition_id, offset_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                reading.event_id,
                reading.sensor_id,
                reading.observed_at,
                reading.temperature_c,
                reading.humidity_pct,
                classify(reading),
                topic,
                partition,
                offset,
            ),
        )
    return cursor.rowcount == 1
