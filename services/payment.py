import json
import aio_pika
import grpc.aio
import asyncio
import os

import order_pb2, order_pb2_grpc

import logging
import time


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('OrderService')


order_channel = 'order:50053'


async def process_payment(message: aio_pika.IncomingMessage):

    async with message.process():
        try:
            data = json.loads(message.body.decode())
            order_id = data['order_id']
            logger.info(f'Processing order {order_id}...')
            
            bank_details = int(data['bank_details'])
            await asyncio.sleep(bank_details)

            async with grpc.aio.insecure_channel(order_channel) as channel:
                stub = order_pb2_grpc.OrderStub(channel)
                
                await stub.UpdateOrder(order_pb2.UpdateOrderRequest(
                    order_id=order_id,
                    status="confirmed"
                ))
                logger.info(f'Order {order_id} confirmed.')

        except Exception as e:
            logger.info(f'Order {order_id} refused: {e}')

            async with grpc.aio.insecure_channel(order_channel) as channel:
                stub = order_pb2_grpc.OrderStub(channel)
                
                await stub.UpdateOrder(order_pb2.UpdateOrderRequest(
                    order_id=order_id,
                    status=f"failed with {e}"
                ))


async def main():
    rabbitmq_url = 'amqp://guest:guest@rabbitmq:5672/'
    logger.info(f"Payment consumer connecting to RabbitMQ at: {rabbitmq_url}")
    
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url)
        
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            
            queue = await channel.declare_queue(
                "payment_queue", 
                durable=True
            )
            
            print("Payment consumer started. Waiting for messages...")
            await queue.consume(process_payment)
            
            # Бесконечное ожидание
            await asyncio.Future()
            
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
        time.sleep(7)
        asyncio.run(main())
        time.sleep(7)
        asyncio.run(main())
        time.sleep(7)
        asyncio.run(main())
        time.sleep(7)
        asyncio.run(main())
        time.sleep(7)
    except KeyboardInterrupt:
        logger.info('Payment Service stopped by user.')