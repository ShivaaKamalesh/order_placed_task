# main.py
import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusMessage

load_dotenv()

SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
TOPIC_NAME = os.getenv("TOPIC_NAME", "orders-task")

app = FastAPI(title="Order Producer - Azure Service Bus")

class OrderItem(BaseModel):
    sku: str
    qty: int
    price: float

class Order(BaseModel):
    order_id: str
    customer_email: EmailStr
    items: list[OrderItem]
    total: float

@app.post("/place-order")
def place_order(order: Order):
    """Send an order message to the topic."""
    try:
        with ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR) as sb_client:
            sender = sb_client.get_topic_sender(topic_name=TOPIC_NAME)
            with sender:
                msg = ServiceBusMessage(json.dumps(order.dict()))
                msg.application_properties = {"type": "order"}
                sender.send_messages(msg)
        return {"status": "Order sent successfully", "order_id": order.order_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send order: {e}")
