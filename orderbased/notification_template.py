# notification_consumer.py
import os
import json
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient

load_dotenv()

SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
TOPIC_NAME = os.getenv("TOPIC_NAME", "orders-task")
SUBSCRIPTION_NAME = "notification-sub"

MAILTRAP_HOST = os.getenv("MAILTRAP_SMTP_HOST")
MAILTRAP_PORT = int(os.getenv("MAILTRAP_SMTP_PORT", "587"))
MAILTRAP_USER = os.getenv("MAILTRAP_USERNAME")
MAILTRAP_PASS = os.getenv("MAILTRAP_PASSWORD")

def send_email(order):
    msg = EmailMessage()
    msg["Subject"] = f"Order {order['order_id']} Confirmation"
    msg["From"] = "no-reply@example.com"
    msg["To"] = order["customer_email"]
    msg.set_content(f"Your order total is ₹{order['total']}")
    with smtplib.SMTP(MAILTRAP_HOST, MAILTRAP_PORT) as smtp:
        smtp.starttls()
        smtp.login(MAILTRAP_USER, MAILTRAP_PASS)
        smtp.send_message(msg)
    print(f"Email sent to {order['customer_email']} (captured in Mailtrap)")

def run_consumer():
    with ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR) as sb_client:
        receiver = sb_client.get_subscription_receiver(
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME
        )
        print(f"{SUBSCRIPTION_NAME} listening for messages...")
        with receiver:
            for msg in receiver:
                try:
                    order = json.loads(b"".join(b for b in msg.body).decode("utf-8"))
                    send_email(order)
                    receiver.complete_message(msg)
                except Exception as e:
                    print("Error:", e)
                    receiver.dead_letter_message(msg)

if __name__ == "__main__":
    run_consumer()
