# create-topic-and-subscriptions.py
"""
Idempotent script to create one topic and three subscriptions.
Uses get_topic/get_subscription + ResourceNotFoundError for existence checks.
"""

import os
from azure.servicebus.management import ServiceBusAdministrationClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
SERVICE_BUS_CONNECTION_STR = ""
if not SERVICE_BUS_CONNECTION_STR:
    raise RuntimeError("Set SERVICE_BUS_CONNECTION_STR environment variable")

TOPIC_NAME = os.getenv("TOPIC_NAME", "orders-task")
SUBSCRIPTIONS = [
    os.getenv("SUB_INVENTORY", "inventory-sub"),
    os.getenv("SUB_PAYMENT", "payment-sub"),
    os.getenv("SUB_SHIPPING", "shipping-sub"),
    os.getenv("SUB_NOTIFICATION","notification-sub")
]

admin = ServiceBusAdministrationClient.from_connection_string(SERVICE_BUS_CONNECTION_STR)

def ensure_topic(topic_name: str):
    try:
        admin.get_topic(topic_name)  # will raise ResourceNotFoundError if not exists
        print(f"Topic exists: {topic_name}")
    except ResourceNotFoundError:
        admin.create_topic(topic_name)
        print(f"Created topic: {topic_name}")

def ensure_subscription(topic_name: str, subscription_name: str):
    try:
        admin.get_subscription(topic_name, subscription_name)
        print(f"Subscription exists: {subscription_name} on topic {topic_name}")
    except ResourceNotFoundError:
        admin.create_subscription(topic_name, subscription_name)
        print(f"Created subscription: {subscription_name} on topic {topic_name}")

def ensure_resources():
    try:
        ensure_topic(TOPIC_NAME)
        for sub in SUBSCRIPTIONS:
            ensure_subscription(TOPIC_NAME, sub)
    except HttpResponseError as e:
        print("HTTP error from Service Bus admin client:", e)
        raise

if __name__ == "__main__":
    ensure_resources()
    print("Bootstrap complete.")