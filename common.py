"""
common.py
---------
Shared utilities for the Maven Market synthetic Kafka producers
(orders_producer.py, inventory_producer.py).

This module owns:
  - loading local credentials from a .env file (cross-platform --
    works the same on Windows PowerShell/cmd, macOS, and Linux)
  - building the Confluent Kafka producer config (SASL_SSL / PLAIN auth)
  - a delivery-report callback for logging acks/failures
  - a graceful-shutdown context so Ctrl+C flushes pending messages
    instead of dropping them silently

Credentials are read from environment variables. In this local/POC
context that means a .env file (see .env.example), loaded automatically
below via python-dotenv -- you do NOT need to manually `export` or
`set` anything in your terminal first. In the real platform, these same
values come from Azure Key Vault via a Databricks secret scope -- the
env var names below intentionally mirror what you'd pull with
dbutils.secrets.get() so the substitution is a 1:1 swap later.
"""

import csv
import json
import logging
import os
import signal
import sys
from contextlib import contextmanager
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

# Loads variables from a .env file in the current working directory (or any
# parent directory) into os.environ. Safe to call even if no .env file
# exists yet -- it just does nothing in that case, and build_producer_config()
# below will raise a clear error naming exactly what's missing.
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def build_producer_config(client_id: str) -> dict:
    """
    Builds the Confluent Kafka producer config dict.

    Required env vars (mirrors POC 4's "connection settings + SASL/JAAS"
    requirement):
        KAFKA_BOOTSTRAP_SERVERS   e.g. pkc-xxxxx.region.azure.confluent.cloud:9092
        KAFKA_API_KEY             Confluent Cloud API key  (== sasl.username)
        KAFKA_API_SECRET          Confluent Cloud API secret (== sasl.password)

    Raises a clear error immediately if anything is missing, rather than
    letting confluent-kafka fail later with an opaque connection error.
    """
    required = ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required Kafka env vars: {missing}. "
            f"Copy .env.example to .env and fill these in "
            f"(in production these come from Key Vault, not a .env file)."
        )

    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "client.id": client_id,
        # Producer-side reliability: wait for full ISR ack before declaring
        # a message delivered. Matters here because the whole point of this
        # POC is proving stream *stability*, not just throughput.
        "acks": "all",
        "retries": 5,
        "linger.ms": 50,
    }


def make_delivery_callback(logger: logging.Logger):
    """Returns a delivery-report callback bound to the given logger."""

    def _on_delivery(err, msg):
        if err is not None:
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            logger.debug(
                "Delivered to %s [partition %d] @ offset %d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    return _on_delivery


def send_json(producer: Producer, topic: str, key: str, payload: dict, callback) -> None:
    """Serializes payload as JSON and produces it, polling to service callbacks."""
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=callback,
    )
    # Non-blocking poll to trigger delivery callbacks for previously
    # produced messages without waiting on this one.
    producer.poll(0)


@contextmanager
def graceful_shutdown(producer: Producer, logger: logging.Logger):
    """
    Context manager that intercepts SIGINT/SIGTERM and flushes the
    producer's outstanding queue before exiting, so a Ctrl+C during a
    demo doesn't silently lose in-flight messages.

    Windows note: SIGINT (Ctrl+C) is fully supported here. SIGTERM can
    be registered without error on Windows too, but nothing on Windows
    actually sends it the way Unix tools do -- in practice, Ctrl+C is
    the only shutdown path you'll use when running these scripts in a
    VS Code terminal on Windows, and that's already covered by SIGINT.
    """
    stop = {"flag": False}

    def _handler(signum, frame):
        logger.info("Shutdown signal received (%s) -- draining producer queue...", signum)
        stop["flag"] = True

    old_sigint = signal.signal(signal.SIGINT, _handler)
    old_sigterm = signal.signal(signal.SIGTERM, _handler)
    try:
        yield stop
    finally:
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d messages still undelivered after flush timeout.", remaining)
        else:
            logger.info("Producer queue flushed cleanly. Exiting.")
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)


def load_ids(csv_path: str, id_column: str) -> list[int]:
    """
    Reads a single integer-ID column out of a master-data CSV
    (e.g. customer_id from Customers.csv, product_id from Products.csv,
    store_id from Stores.csv). Shared by both producers so "what counts
    as a real entity" is defined in exactly one place.
    """
    path = Path(csv_path)
    if not path.is_absolute():
        candidates = [
            Path.cwd() / path,
            Path(__file__).resolve().parent / path,
            Path(__file__).resolve().parent.parent / path,
        ]
        for candidate in candidates:
            if candidate.exists():
                path = candidate
                break

    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        ids = [int(row[id_column]) for row in reader if row.get(id_column)]
    if not ids:
        raise ValueError(f"No values found in column '{id_column}' of {path}")
    return ids


def fail_fast(msg: str) -> None:
    logging.getLogger(__name__).error(msg)
    sys.exit(1)
