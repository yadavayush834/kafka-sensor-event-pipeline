"""Show the stored readings and alert counts."""

import argparse
import os
import sqlite3


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect processed sensor readings")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("limit must be positive")
    path = os.getenv("DATABASE_PATH", "readings.db")
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.OperationalError as exc:
        parser.error(f"cannot open database at {path}: {exc}")
    with connection:
        rows = connection.execute(
            """SELECT sensor_id, observed_at, temperature_c, humidity_pct,
                      status, partition_id, offset_id
               FROM readings ORDER BY rowid DESC LIMIT ?""",
            (args.limit,),
        ).fetchall()
        counts = connection.execute(
            "SELECT status, COUNT(*) FROM readings GROUP BY status ORDER BY status"
        ).fetchall()
    connection.close()
    for sensor, observed, temperature, humidity, status, partition, offset in rows:
        print(f"{observed} {sensor:<10} {temperature:5.1f} C {humidity:5.1f}% {status:<6} p={partition} o={offset}")
    print(f"total={sum(count for _, count in counts)} " + " ".join(f"{status.lower()}={count}" for status, count in counts))


if __name__ == "__main__":
    main()
