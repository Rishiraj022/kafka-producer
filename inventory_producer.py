"""
inventory_producer.py
----------------------
Simulates the missing "real-time Inventory" Kafka feed. Unlike Orders,
there is no real inventory dataset to replay at all -- this is a fully
synthetic stock-level generator, seeded with real store_id and
product_id values pulled from MavenMarket_Stores.csv and
MavenMarket_Products.csv so the events at least reference real entities.

WHY THIS EXISTS: documented gap-fill for the Kafka Inventory topic.
State (current quantity_on_hand per store/product combo) is held
in-memory for the life of the process -- this is a generator script,
not a system of record, so that's an acceptable simplification for a
capstone POC. Document this explicitly if asked.

Usage:
    python inventory_producer.py \\
        --stores-csv "../Data/SPTStore Data Files/MavenMarket_Stores.csv" \\
        --products-csv "../Data/SPTStore Data Files/MavenMarket_Products.csv" \\
        --topic inventory \\
        --num-combos 300 \\
        --rate 3 \\
        --duration 600

Each message:
    key   = "<store_id>-<product_id>"
    value = JSON payload, see build_inventory_payload()
"""

import argparse
import logging
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from common import (
    build_producer_config,
    graceful_shutdown,
    load_ids,
    make_delivery_callback,
    send_json,
)

logger = logging.getLogger("inventory_producer")

RESTOCK_THRESHOLD = 20      # below this on-hand level, a restock becomes likely
SALE_QTY_RANGE = (1, 5)     # units removed per simulated sale
RESTOCK_QTY_RANGE = (50, 200)  # units added per simulated restock
INITIAL_QTY_RANGE = (50, 300)


def build_combos(store_ids: list[int], product_ids: list[int], num_combos: int) -> list[tuple]:
    """
    Samples a manageable subset of (store_id, product_id) pairs rather than
    the full cross-product (24 stores x 1,560 products = 37,440 -- far more
    than needed to demonstrate the streaming pattern).
    """
    all_possible = [(s, p) for s in store_ids for p in product_ids]
    sample_size = min(num_combos, len(all_possible))
    combos = random.sample(all_possible, sample_size)
    logger.info(
        "Tracking %d (store, product) combos out of %d possible.",
        len(combos), len(all_possible),
    )
    return combos


def init_state(combos: list[tuple]) -> dict:
    return {
        combo: random.randint(*INITIAL_QTY_RANGE)
        for combo in combos
    }


def build_inventory_payload(store_id: int, product_id: int, event_type: str,
                             delta: int, qty_on_hand: int) -> dict:
    return {
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "store_id": store_id,
        "product_id": product_id,
        "event_type": event_type,        # "SALE" or "RESTOCK"
        "quantity_delta": delta,
        "quantity_on_hand": qty_on_hand,
    }


def tick(state: dict, combos: list[tuple], events_per_tick: int) -> list[dict]:
    """Generates one batch of inventory events by mutating a random sample of combos."""
    chosen = random.sample(combos, min(events_per_tick, len(combos)))
    events = []
    for (store_id, product_id) in chosen:
        current_qty = state[(store_id, product_id)]

        if current_qty < RESTOCK_THRESHOLD and random.random() < 0.7:
            delta = random.randint(*RESTOCK_QTY_RANGE)
            event_type = "RESTOCK"
        else:
            max_sale = min(SALE_QTY_RANGE[1], current_qty) if current_qty > 0 else 0
            delta = -random.randint(SALE_QTY_RANGE[0], max(max_sale, SALE_QTY_RANGE[0]))
            delta = max(delta, -current_qty)  # never go negative
            event_type = "SALE"

        new_qty = current_qty + delta
        state[(store_id, product_id)] = new_qty
        events.append(build_inventory_payload(store_id, product_id, event_type, delta, new_qty))

    return events


def run(producer: Producer, topic: str, state: dict, combos: list[tuple],
        rate_per_sec: float, events_per_tick: int, duration: int | None):
    callback = make_delivery_callback(logger)
    base_interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    sent = 0
    start = time.time()

    with graceful_shutdown(producer, logger) as stop:
        while True:
            if stop["flag"]:
                logger.info("Stopping after %d messages.", sent)
                break
            if duration is not None and (time.time() - start) >= duration:
                logger.info("Reached --duration (%ds). Stopping.", duration)
                break

            for event in tick(state, combos, events_per_tick):
                key = f"{event['store_id']}-{event['product_id']}"
                send_json(producer, topic=topic, key=key, payload=event, callback=callback)
                sent += 1

            if sent % 200 == 0:
                logger.info("Sent %d inventory events so far...", sent)

            if base_interval > 0:
                time.sleep(random.uniform(0.7, 1.3) * base_interval)

    producer.flush()
    logger.info("Done. Total inventory events sent: %d", sent)


def parse_args():
    p = argparse.ArgumentParser(description="Synthetic Inventory Kafka producer")
    p.add_argument("--stores-csv", required=True, help="Path to MavenMarket_Stores.csv")
    p.add_argument("--products-csv", required=True, help="Path to MavenMarket_Products.csv")
    p.add_argument(
        "--topic", default=os.environ.get("KAFKA_INVENTORY_TOPIC", "inventory"),
        help="Kafka topic to publish to (default: $KAFKA_INVENTORY_TOPIC or 'inventory')",
    )
    p.add_argument("--num-combos", type=int, default=300,
                    help="Number of (store, product) pairs to simulate (default: 300)")
    p.add_argument("--events-per-tick", type=int, default=10,
                    help="How many combos get an event each tick (default: 10)")
    p.add_argument("--rate", type=float, default=3.0,
                    help="Ticks per second (default: 3)")
    p.add_argument("--duration", type=int, default=None,
                    help="Stop after this many seconds (omit to run until Ctrl+C)")
    p.add_argument("--seed", type=int, default=None, help="Random seed, for reproducible demo runs")
    return p.parse_args()


def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    store_ids = load_ids(args.stores_csv, "store_id")
    product_ids = load_ids(args.products_csv, "product_id")
    combos = build_combos(store_ids, product_ids, args.num_combos)
    state = init_state(combos)

    config = build_producer_config(client_id="inventory-producer")
    producer = Producer(config)

    logger.info(
        "Starting Inventory producer -> topic='%s' combos=%d rate=%.2f/s duration=%s",
        args.topic, len(combos), args.rate, args.duration,
    )
    run(producer, args.topic, state, combos, args.rate, args.events_per_tick, args.duration)


if __name__ == "__main__":
    main()
