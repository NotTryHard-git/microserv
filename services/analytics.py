from concurrent import futures
import json
from confluent_kafka import Producer, Consumer
import time

import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('AnalyticsService')

orders = {} # orders[order_id] = status

def analysis():
    statuses = {'in processing': 0, 'failed': 0, 'confirmed': 0}
    for status in orders.values():
        if status not in statuses: # failed
            statuses['failed'] += 1
        else:
            statuses[status] += 1
    return statuses

def main():
    global orders
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'order-listener',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
        'session.timeout.ms': 6000
    }
    time.sleep(30)
    consumer = Consumer(conf)
    consumer.subscribe(['orders'])
    
    logger.info("Waiting for messages...")
    while True:
        msg = consumer.poll(timeout=0.1)
        if msg is None:
            continue
        if msg.error():
            logger.warning(f"Kafka message error: {msg.error()}")
        else:
            logger.info("Message is recieved.")
            data = msg.value().decode()
            data = json.loads(data)

            if data['action'] == 'created':
                order = data['order']
                order_id = order['order_id']
                status = order['status']
                logger.info(f"Order {order['order_id']} created.")

            elif data['action'] == 'updated':
                order_id = msg.key().decode()
                status = data['status']
                logger.info(f"Order {order['order_id']} status updated: {status}")

            orders[order_id] = status
            statuses = analysis()
            logger.info(f"\n\n{statuses['in processing']} orders in processing.\n{statuses['confirmed']} confirmed orders.\n{statuses['failed']} failed orders.\n\n")
                


if __name__ == '__main__':
    logger.info("Analysis server initializing...")
    try:
        main()
    except Exception as e:
        logger.warning(f"Analysis server failed. Error: {e}")