from kafka import KafkaProducer
import json
import time
from retailsales import send_retail_sales_data
from retailsales2 import send_retail_sales_data2

EVENT_NAME = "onlinesales"
EVENT_HUB_HOST = "onlineeventspace.servicebus.windows.net:9093"

CONNECTION_STRING = (
    "Endpoint=sb://onlineeventspace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Id1h66wvEMzJ3ifHUIkWvVLOAZImkGrBT+AEhBbUT/Y="
)

producer = KafkaProducer(
    bootstrap_servers=[EVENT_HUB_HOST],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

if __name__ == "__main__":
    try:
        while True:
            event1 = send_retail_sales_data()
            event2 = send_retail_sales_data2()

            producer.send(EVENT_NAME, value=event1)
            producer.send(EVENT_NAME, value=event2)

            print(f"Sent event1: {event1}")
            print(f"Sent event2: {event2}")

            producer.flush()
            time.sleep(1)

    except Exception as e:
        print("Error while sending events:", e)

    finally:
        producer.close()