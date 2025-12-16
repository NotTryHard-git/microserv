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

# Circuit Breaker
FAIL_THRESHOLD = 3

OPEN_STATE_TIME = 90 # secs

fails = 0
open_state_end = -1


async def process_payment(message: aio_pika.IncomingMessage):
    global fails, open_state_end
    if time.time() < open_state_end:
        await asyncio.sleep(open_state_end - time.time() + 0.2) # чтобы точно не начать раньше конца open state
        logger.info("Slept until the end of open state.")

    async with message.process():
        try:
            data = json.loads(message.body.decode())
            order_id = data['order_id']
            logger.info(f'Processing order {order_id}...')
            raise Exception("Имитация ошибки платежа") # иммитируем ошибки
            bank_details = int(data['bank_details'])
            await asyncio.sleep(bank_details)

            fails = 0 # сброс подсчёта ошибок при успешной обработке платежа
            
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
            
            fails += 1
            if fails >= FAIL_THRESHOLD:
                logger.info("----------------------- OPEN STATE -----------------------")
                open_state_end = time.time() + OPEN_STATE_TIME
                fails = 0


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
        for _ in range(5):
            time.sleep(10)
            asyncio.run(main())
        logger.info("Connection to RabbitMQ is failed.")
    except KeyboardInterrupt:
        logger.info('Payment Service stopped by user.')