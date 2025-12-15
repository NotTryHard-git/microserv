from concurrent import futures
import grpc
import aio_pika
import json
import asyncio

import order_pb2, order_pb2_grpc
import auth_pb2, auth_pb2_grpc
import catalog_pb2, catalog_pb2_grpc
import notification_pb2, notification_pb2_grpc

import logging
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('OrderService')

class OrderService(order_pb2_grpc.OrderServicer):

    def __init__(self, rabbitmq_conn):
        logger.info('Initializing Order Service...')

        self.rabbitmq_conn = rabbitmq_conn

        self.carts = {}
        # carts[uid] = {cart_product_id: {product_id, quantity}  }
        self.user_order = {}
        # user_order[uid] = [order_id, ...]
        self.orders = {}
        # orders[order_id] = {order_id, uid, product_id, quantity, price, bank_details, status}

        self.catalog_channel = grpc.insecure_channel('catalog:50052')
        self.catalog_stub = catalog_pb2_grpc.CatalogStub(self.catalog_channel)

        self.auth_channel = grpc.insecure_channel('auth:50051')
        self.auth_stub = auth_pb2_grpc.AuthStub(self.auth_channel)

        self.notification_channel = grpc.insecure_channel('notification:50055')
        self.notification_stub = notification_pb2_grpc.NotificationStub(self.notification_channel)
        
        
        logger.info("Order Service initialized successfully")
    
    def GetCart(self, request, context):
        logger.info('Getting Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.GetCartResponse()
            self.carts[uid] = {}

        return order_pb2.GetCartResponse(
            products = [order_pb2.Product(cart_product_id=cart_product_id, **self.carts[request.uid][cart_product_id]) for cart_product_id in self.carts[request.uid]]
        )
    
    def GetFromCart(self, request, context):
        logger.info('Getting From Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.Product()
            self.carts[uid] = {}
        
        if request.cart_product_id not in self.carts[uid]:
            logger.warning("This product doesn't exist in cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in cart.")
            return order_pb2.Product()
        
        return order_pb2.Product(cart_product_id=request.cart_product_id, **self.carts[uid][request.cart_product_id])
    
    def AddToCart(self, request, context):
        logger.info('Adding To Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}

        try:
            catalog_resp = self.catalog_stub.GetProduct(catalog_pb2.GetProductRequest(
                product_id = request.product_id
            ))
            product_id, quantity = catalog_resp.product_id, catalog_resp.quantity
        except Exception as e:
            product_id = ''
            quantity = 0
        if not product_id:
            logger.warning("This product doesn't exist.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist.")
            return order_pb2.SuccessResponse(success=False)
        if request.quantity > quantity:
            logger.warning("Quantity of this product is out of range.")
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Quantity of this product is out of range.")
            return order_pb2.SuccessResponse(success=False)
        
        cart_product_id = str(uuid.uuid4())
        self.carts[request.uid][cart_product_id] = {'product_id': request.product_id, 'quantity': request.quantity}
        return order_pb2.SuccessResponse(success=True)
    
    def DeleteFromCart(self, request, context):
        logger.info('Deleting From Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}
        if request.cart_product_id not in self.carts[request.uid]:
            logger.warning("This product doesn't exist in this cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in this cart.")
            return order_pb2.SuccessResponse(success=False)
        del self.carts[request.uid][request.cart_product_id]
        return order_pb2.SuccessResponse(success=True)
    
    def UpdateWithinCart(self, request, context):
        logger.info('Updating Within Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}

        if request.cart_product_id not in self.carts[request.uid]:
            logger.warning("This product doesn't exist in this cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in this cart.")
            return order_pb2.SuccessResponse(success=False)
        
        try:
            catalog_resp = self.catalog_stub.GetProduct(catalog_pb2.GetProductRequest(
                product_id = self.carts[request.uid][request.cart_product_id]['product_id']
            ))
            quantity = catalog_resp.quantity
        except Exception as e:
            logger.warning("Updating Within Cart Failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Updating Within Cart Failed.")
            return order_pb2.SuccessResponse(success=False)
        
        if request.quantity > quantity:
            logger.warning("Quantity of this product is out of range.")
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Quantity of this product is out of range.")
            return order_pb2.SuccessResponse(success=False)
        
        self.carts[request.uid][request.cart_product_id]['quantity'] = request.quantity
        return order_pb2.SuccessResponse(success=True)
    
    async def BuyFromCart(self, request, context):
        logger.info('Buying From Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
                logger.warning("Pulling uid from Auth failed.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Pulling uid from Auth failed.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}

        if request.cart_product_id not in self.carts[request.uid]:
            logger.warning("This product doesn't exist in this cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in this cart.")
            return order_pb2.SuccessResponse(success=False)
        
        try:
            catalog_resp = self.catalog_stub.GetProduct(catalog_pb2.GetProductRequest(
                product_id = self.carts[request.uid][request.cart_product_id]['product_id']
            ))
            quantity = catalog_resp.quantity
            price = catalog_resp.price
        except Exception as e:
            logger.warning("Pulling product from Catalog failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Pulling product from Catalog failed.")
            return order_pb2.SuccessResponse(success=False)
        
        if self.carts[request.uid][request.cart_product_id]['quantity'] > quantity:
            logger.warning("Quantity of this product is out of range.")
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Quantity of this product is out of range.")
            return order_pb2.SuccessResponse(success=False)
        
        # creating order
        uid = request.uid
        order_id = str(uuid.uuid4())
        if uid not in self.user_order:
            self.user_order[uid] = []
        self.user_order[uid].append(order_id)
        self.orders[order_id] = {
            'order_id': order_id,
            'uid': uid,
            'product_id': self.carts[uid][request.cart_product_id]['product_id'],
            'quantity': self.carts[uid][request.cart_product_id]['quantity'],
            'price': price,
            'bank_details': request.bank_details,
            'status': 'in processing'
        }

        try:

            await self.send_to_rabbitmq(
                "payment_queue",
                self.orders[order_id]
            )
            logger.info('Order is sent to RabbitMQ')

        except Exception as e:
            logger.warning("Pushing order to RabbitMQ failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Pushing order to RabbitMQ failed.")
            self.orders[order_id]['status'] = 'failed'
            self.notification_stub.CreateNotification(notification_pb2.CreateNotificationRequest(
                uid = uid,
                order_id = order_id,
                status = 'failed'
            ))
            return order_pb2.SuccessResponse(success=False)

        self.notification_stub.CreateNotification(notification_pb2.CreateNotificationRequest(
            uid = uid,
            order_id = order_id,
            status = 'in processing'
        ))

        logger.info("Notification created.")
        
        del self.carts[request.uid][request.cart_product_id]
        return order_pb2.SuccessResponse(success=True)
    
    def GetUserOrders(self, request, context):
        logger.info('Getting User Orders...')

        if request.uid not in self.user_order:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.Orders()
            self.user_order[uid] = []

        return order_pb2.Orders(
            orders = [order_pb2.OrderResponse(**self.orders[order_id]) for order_id in self.user_order[request.uid]]
        )
    
    def GetOrder(self, request, context):
        logger.info('Getting Order...')
        if request.order_id not in self.orders:
            logger.warning("This order doesn't exist.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This order doesn't exist.")
            return order_pb2.OrderResponse()
        return order_pb2.OrderResponse(**self.orders[request.order_id])
    
    def UpdateOrder(self, request, context):
        logger.info(f"Updating Order {request.order_id}: status {request.status}")
        self.orders[request.order_id]['status'] = request.status

        order = self.orders[request.order_id]
        if request.status != 'confirmed': # Saga rollback

            cart_product_id = str(uuid.uuid4())
            self.carts[order['uid']][cart_product_id] = {'product_id': order['product_id'], 'quantity': order['quantity']}
        else:
            product = self.catalog_stub.GetProduct(catalog_pb2.GetProductRequest(product_id=order['product_id']))

            self.catalog_stub.UpdateProduct(catalog_pb2.UpdateProductRequest(
                product_id=order['product_id'],
                name=product.name,
                desc=product.desc,
                price=product.price,
                category_id=product.category_id,
                quantity=product.quantity - order['quantity']
            ))

        self.notification_stub.CreateNotification(notification_pb2.CreateNotificationRequest(
            uid = order['uid'],
            order_id = request.order_id,
            status = request.status
        ))

        return order_pb2.Empty() 
    

    async def send_to_rabbitmq(self, queue, message):
        logger.info('Sending to RabbitMQ...............')
        conn = await aio_pika.connect_robust(self.rabbitmq_conn)

        async with conn:
            channel = await conn.channel()

            await channel.declare_queue(queue, durable=True)

            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode()
                ),
                routing_key=queue
            )
        


async def serve():
    logger.info('Starting Order Service...')
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))

    order_pb2_grpc.add_OrderServicer_to_server(
        OrderService(rabbitmq_conn='amqp://guest:guest@rabbitmq:5672/'),
        server
        )
    
    server.add_insecure_port("[::]:50053")
    logger.info('Order Service successfully started on port 50053.')
    await server.start()
    await server.wait_for_termination()


if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info('Order Service stopped by user.')
    except Exception as e:
        logger.error(f'Order Service crashed: {str(e)}')