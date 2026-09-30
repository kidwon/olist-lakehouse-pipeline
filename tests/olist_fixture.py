"""A small, clean, deterministic dataset in the exact Olist Kaggle CSV layout.

Tests run on this instead of the real dump, so CI needs no Kaggle credentials and every
anomaly in the pipeline's output can be traced to the replay injector (the base data has none).
"""

from __future__ import annotations

import csv
import datetime as dt
import random
from pathlib import Path

STATES = [("sao paulo", "SP"), ("rio de janeiro", "RJ"), ("campinas", "SP"), ("salvador", "BA"), ("fortaleza", "CE")]
CATEGORIES = [("beleza_saude", "health_beauty"), ("informatica_acessorios", "computers_accessories"), ("esporte_lazer", "sports_leisure")]


def _ts(t: dt.datetime | None) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S") if t else ""


def _write(path: Path, header: list[str], rows: list[list], bom: bool = False) -> None:
    with path.open("w", newline="", encoding="utf-8-sig" if bom else "utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def write_fixture(target: Path, n_orders: int = 3000, start: dt.date = dt.date(2018, 5, 20), days: int = 18, seed: int = 7) -> None:
    rnd = random.Random(seed)
    target.mkdir(parents=True, exist_ok=True)

    sellers = [f"seller{i:03d}" for i in range(300)]  # enough for ~2% to relocate
    products = [f"product{i:03d}" for i in range(60)]
    persons = [f"person{i:04d}" for i in range(int(n_orders * 0.8))]  # some people order more than once

    _write(target / "olist_sellers_dataset.csv", ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
           [[s, f"{10000 + i}", *STATES[i % len(STATES)]] for i, s in enumerate(sellers)])
    _write(target / "olist_products_dataset.csv",
           ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty",
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
           [[p, CATEGORIES[i % 3][0], 40, 300, 1 + i % 4, 100 * (1 + i % 9), 20, 10, 15] for i, p in enumerate(products)])
    _write(target / "product_category_name_translation.csv", ["product_category_name", "product_category_name_english"],
           [list(c) for c in CATEGORIES], bom=True)  # the real file has a BOM too

    orders, items, customers, reviews = [], [], [], []
    for n in range(n_orders):
        oid, cid = f"order{n:05d}", f"cust{n:05d}"
        purchase = dt.datetime.combine(start + dt.timedelta(days=n % days), dt.time()) + dt.timedelta(minutes=rnd.randrange(24 * 60))
        estimated = purchase + dt.timedelta(days=8)
        roll = rnd.random()
        approved = purchase + dt.timedelta(hours=1)
        carrier = purchase + dt.timedelta(days=1) if roll > 0.05 else None
        delivered = purchase + dt.timedelta(days=rnd.randint(3, 10)) if carrier and roll > 0.15 else None
        status = "canceled" if roll <= 0.05 else ("delivered" if delivered else "shipped")
        orders.append([oid, cid, status, _ts(purchase), _ts(approved), _ts(carrier), _ts(delivered), _ts(estimated)])
        city, state = STATES[n % len(STATES)]
        customers.append([cid, rnd.choice(persons), f"{20000 + n % 500}", city, state])
        for i in range(1, rnd.choice([1, 1, 2, 3]) + 1):
            items.append([oid, i, rnd.choice(products), rnd.choice(sellers), _ts(purchase + dt.timedelta(days=3)),
                          f"{rnd.uniform(10, 500):.2f}", f"{rnd.uniform(5, 40):.2f}"])
        if delivered:
            created = dt.datetime.combine(delivered.date() + dt.timedelta(days=1), dt.time())
            reviews.append([f"review{n:05d}", oid, rnd.randint(1, 5), "", "", _ts(created), _ts(created + dt.timedelta(days=1))])

    _write(target / "olist_orders_dataset.csv",
           ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"], orders)
    _write(target / "olist_order_items_dataset.csv",
           ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"], items)
    _write(target / "olist_customers_dataset.csv",
           ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"], customers)
    _write(target / "olist_order_reviews_dataset.csv",
           ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp"], reviews)
