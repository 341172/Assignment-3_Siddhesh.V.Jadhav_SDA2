"""
Member 1 - Purchase Fraud & High-Value Transaction Monitoring
Retail FMCD Industry

Generates simulated POS / E-commerce purchase transactions (including
occasional refund events and rapid repeat purchases) and streams them
into the Kafka topic 'customer-purchase-data'.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = "customer-purchase-data"

PRODUCTS = [
    {"id": "P1001", "name": "55-inch Smart LED TV", "category": "Television", "price": 42999},
    {"id": "P1002", "name": "Double Door Refrigerator 260L", "category": "Refrigerator", "price": 28999},
    {"id": "P1003", "name": "1.5 Ton Split Inverter AC", "category": "Air Conditioner", "price": 34999},
    {"id": "P1004", "name": "Front Load Washing Machine 7Kg", "category": "Washing Machine", "price": 26999},
    {"id": "P1005", "name": "Microwave Oven 23L Convection", "category": "Kitchen Appliance", "price": 10999},
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "EMI"]
CHANNELS = ["In-Store", "Website", "Mobile App"]
STORES = ["Store-Jaipur-01", "Store-Mumbai-04", "Store-Bangalore-02", "Online"]

CUSTOMER_IDS = [f"CUST{1000 + i}" for i in range(200)]
# a small pool of "risky" customers who will occasionally trigger
# rapid repeat-purchase behaviour, to make sure our fraud alert fires
RISKY_CUSTOMERS = CUSTOMER_IDS[:2]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def make_transaction(customer_id=None, amount_override=None, event_type="purchase_transaction"):
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 2)
    amount = amount_override if amount_override else product["price"] * qty
    return {
        "event_type": event_type,
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id or random.choice(CUSTOMER_IDS),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": qty,
        "amount": amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "channel": random.choice(CHANNELS),
        "store": random.choice(STORES),
        "order_status": "Completed" if event_type == "purchase_transaction" else "Refunded",
        "timestamp": now_iso(),
    }


def main():
    print(f"Starting Member 1 producer -> topic '{TOPIC}' (Ctrl+C to stop)\n")
    count = 0
    try:
        while True:
            roll = random.random()

            if roll < 0.08:
                # simulate a high-value transaction (potential fraud trigger)
                record = make_transaction(amount_override=random.randint(45000, 90000))
            elif roll < 0.13:
                # simulate a refund event
                record = make_transaction(event_type="refund_event")
            elif roll < 0.25:
                # simulate rapid repeat purchases from the same "risky" customer
                record = make_transaction(customer_id=random.choice(RISKY_CUSTOMERS))
            else:
                record = make_transaction()

            producer.send(TOPIC, value=record)
            count += 1
            print(f"[{count}] {record['event_type']} | {record['customer_id']} | Rs.{record['amount']}")

            time.sleep(random.uniform(0.3, 1.0))

    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()