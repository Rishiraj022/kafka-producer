"""
orders_producer.py
-------------------
Generates synthetic, BRAND-NEW order events for the missing "real-time
Orders" Kafka feed -- this does NOT replay MavenMarket_Transactions_*.csv.
Those files are already the authoritative historical record, loaded once
via Auto Loader into bronze.transactions_raw. Replaying them into a
"real-time" topic would double-count the same sales facts through two
different ingestion paths, which is architecturally wrong.

Instead, each event here is a freshly invented order, built with respect
to the real master-data ID universes:
    - store_id    drawn from MavenMarket_Stores.csv
    - product_id  drawn from MavenMarket_Products.csv
    - customer_id drawn from MavenMarket_Customers.csv, MOST of the time

The "most of the time" matters: a small, configurable fraction of orders
deliberately use a customer_id that does NOT exist in Customers.csv --
simulating a brand-new walk-in customer whose record hasn't reached
MongoDB/Bronze yet. This is the exact early-arriving-fact scenario the
Silver-layer inferred-member pattern is designed to catch, so this
producer and that pipeline logic are meant to be demonstrated together.

Usage:
    python orders_producer.py \\
        --stores-csv "Data/SPTStore Data Files/MavenMarket_Stores.csv" \\
        --products-csv "Data/SPTStore Data Files/MavenMarket_Products.csv" \\
        --customers-csv "Data/SPTStore Data Files/MavenMarket_Customers.csv" \\
        --topic orders \\
        --rate 5 \\
        --new-customer-rate 0.05 \\
        --duration 600

Each message:
    key   = store_id
    value = JSON payload, see build_order_payload()
"""

import argparse
import logging
import os
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from common import (
    build_producer_config,
    graceful_shutdown,
    load_ids,
    make_delivery_callback,
    send_json,
)

logger = logging.getLogger("orders_producer")

QTY_RANGE = (1, 10)
# Observed directly from MavenMarket_Transactions_1997.csv: stock_date is
# always 1-7 days before transaction_date, fairly evenly distributed across
# that range. Reusing the same distribution here keeps stock_date
# (inventory dwell time) analytically comparable between historical
# (batch) and new (streaming) order facts in Gold.
STOCK_LAG_DAYS_RANGE = (1, 7)
NEW_CUSTOMER_BUFFER_SIZE = 20      # how many recently-minted new customers to remember
REUSE_NEW_CUSTOMER_PROBABILITY = 0.5  # chance a "new customer" event reuses a recent one
                                        # instead of minting yet another brand-new id


class CustomerIdAllocator:
    """
    Decides which customer_id to attach to a synthetic order.

    ~`new_customer_rate` of the time, it mints or reuses an id that is
    NOT in the real Customers.csv universe -- a customer who doesn't
    exist in master data yet. The rest of the time it picks a real,
    known customer_id.

    Minted ids start at max(known_ids) + 1 and increment, so they're
    easy to spot in Bronze/Silver as "new" without needing any special
    flag in the payload itself (a real POS system wouldn't know whether
    a customer_id exists in the master data system -- that's discovered
    downstream, in Silver, via the dim_customer join. This generator
    intentionally doesn't leak that ground truth into the event).
    """

    def __init__(self, known_customer_ids: list[int], new_customer_rate: float):
        self.known_ids = known_customer_ids
        self.new_customer_rate = new_customer_rate
        self._next_new_id = max(known_customer_ids) + 1
        self._recent_new_ids: deque = deque(maxlen=NEW_CUSTOMER_BUFFER_SIZE)

    def pick(self) -> tuple[int, bool]:
        """Returns (customer_id, is_new) -- is_new is for local logging only,
        never written into the Kafka payload."""
        if random.random() < self.new_customer_rate:
            if self._recent_new_ids and random.random() < REUSE_NEW_CUSTOMER_PROBABILITY:
                return random.choice(self._recent_new_ids), True
            new_id = self._next_new_id
            self._next_new_id += 1
            self._recent_new_ids.append(new_id)
            return new_id, True
        return random.choice(self.known_ids), False


def build_order_payload(store_id: int, product_id: int, customer_id: int, quantity: int) -> dict:
    """
    Schema is aligned with the historical MavenMarket_Transactions_*.csv
    columns (transaction_date, stock_date, product_id, customer_id,
    store_id, quantity) so Silver can conform both sources into one
    fact_sales shape without inventing a parallel set of column names.

    Two fields the historical CSVs don't have are kept as streaming-only
    metadata, since there's no equivalent in a flat historical file:
      - order_id: the batch source has no natural order identifier
      - event_ts: actual Kafka arrival time, distinct from transaction_date
        (matters for the checkpoint-recovery POC)

    transaction_date/stock_date are emitted as ISO 8601 (YYYY-MM-DD)
    rather than the historical file's M/D/YYYY strings -- the right
    format for a new JSON event source. Silver already needs
    source-specific date parsing for the batch CSVs regardless; this
    doesn't add work beyond what already exists for that source.
    """
    now = datetime.now(timezone.utc)
    transaction_date = now.date()
    stock_lag_days = random.randint(*STOCK_LAG_DAYS_RANGE)
    stock_date = transaction_date - timedelta(days=stock_lag_days)

    return {
        "order_id": str(uuid.uuid4()),
        "event_ts": now.isoformat(),
        "transaction_date": transaction_date.isoformat(),
        "stock_date": stock_date.isoformat(),
        "store_id": store_id,
        "product_id": product_id,
        "customer_id": customer_id,
        "quantity": quantity,
    }


def run(
    producer: Producer,
    topic: str,
    store_ids: list[int],
    product_ids: list[int],
    customer_allocator: CustomerIdAllocator,
    rate_per_sec: float,
    duration: int | None,
):
    callback = make_delivery_callback(logger)
    base_interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    sent = 0
    new_customer_events = 0
    start = time.time()

    with graceful_shutdown(producer, logger) as stop:
        while True:
            if stop["flag"]:
                logger.info("Stopping after %d messages.", sent)
                break
            if duration is not None and (time.time() - start) >= duration:
                logger.info("Reached --duration (%ds). Stopping.", duration)
                break

            store_id = random.choice(store_ids)
            product_id = random.choice(product_ids)
            customer_id, is_new = customer_allocator.pick()
            quantity = random.randint(*QTY_RANGE)

            payload = build_order_payload(store_id, product_id, customer_id, quantity)
            send_json(producer, topic=topic, key=str(store_id), payload=payload, callback=callback)
            sent += 1

            if is_new:
                new_customer_events += 1
                logger.info(
                    "Order %s uses customer_id=%d, which is NOT in the known "
                    "customer master list (simulated new walk-in customer).",
                    payload["order_id"], customer_id,
                )

            if sent % 200 == 0:
                logger.info(
                    "Sent %d order events so far (%d involved an unrecognized customer_id).",
                    sent, new_customer_events,
                )

            if base_interval > 0:
                time.sleep(random.uniform(0.5, 1.5) * base_interval)

    producer.flush()
    logger.info(
        "Done. Total order events sent: %d (%d with an unrecognized customer_id).",
        sent, new_customer_events,
    )


def parse_args():
    p = argparse.ArgumentParser(description="Synthetic Orders Kafka producer")
    p.add_argument("--stores-csv", required=True, help="Path to MavenMarket_Stores.csv")
    p.add_argument("--products-csv", required=True, help="Path to MavenMarket_Products.csv")
    p.add_argument("--customers-csv", required=True, help="Path to MavenMarket_Customers.csv")
    p.add_argument(
        "--topic", default=os.environ.get("KAFKA_ORDERS_TOPIC", "orders"),
        help="Kafka topic to publish to (default: $KAFKA_ORDERS_TOPIC or 'orders')",
    )
    p.add_argument("--rate", type=float, default=5.0, help="Orders per second (default: 5)")
    p.add_argument(
        "--new-customer-rate", type=float, default=0.05,
        help="Fraction of orders (0.0-1.0) that use a customer_id not present in "
             "Customers.csv, simulating a new walk-in customer (default: 0.05)",
    )
    p.add_argument(
        "--duration", type=int, default=None,
        help="Stop after this many seconds (omit to run until Ctrl+C)",
    )
    p.add_argument("--seed", type=int, default=None, help="Random seed, for reproducible demo runs")
    return p.parse_args()


def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    store_ids = load_ids(args.stores_csv, "store_id")
    product_ids = load_ids(args.products_csv, "product_id")
    customer_ids = load_ids(args.customers_csv, "customer_id")
    allocator = CustomerIdAllocator(customer_ids, args.new_customer_rate)

    config = build_producer_config(client_id="orders-producer")
    producer = Producer(config)

    logger.info(
        "Starting Orders producer -> topic='%s' rate=%.2f/s new_customer_rate=%.2f duration=%s",
        args.topic, args.rate, args.new_customer_rate, args.duration,
    )
    run(producer, args.topic, store_ids, product_ids, allocator, args.rate, args.duration)


if __name__ == "__main__":
    main()