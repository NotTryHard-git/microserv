import grpc
from concurrent import futures
import logging
import uuid
import datetime
import pika
import json
import threading
from typing import Dict, List
import time
import order_pb2
import order_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('OrderService')

class OrderService(order_pb2_grpc.OrderServiceServicer):
    def __init__(self):
        self.orders: Dict[str, dict] = {}
        self.order_counter = 1000
        self.rabbitmq_connection = None
        self.channel = None
        self.setup_rabbitmq()
        logger.info("Order Service initialized with Saga support")
        
        # Initialize with sample data
        self._initialize_sample_data()

    def _initialize_sample_data(self):
        """Initialize with sample orders"""
        sample_orders = [
            {
                'user_id': 'sample_user_1',
                'status': 'DELIVERED',
                'items': [
                    {
                        'product_id': 'prod_001',
                        'product_name': 'iPhone 13',
                        'price': 999.99,
                        'quantity': 1,
                        'total': 999.99
                    }
                ],
                'total_amount': 1099.99,
                'shipping_address': '123 Main St, New York, NY'
            },
            {
                'user_id': 'sample_user_2',
                'status': 'PROCESSING',
                'items': [
                    {
                        'product_id': 'prod_002',
                        'product_name': 'MacBook Pro',
                        'price': 1999.99,
                        'quantity': 1,
                        'total': 1999.99
                    }
                ],
                'total_amount': 2199.99,
                'shipping_address': '456 Oak Ave, Los Angeles, CA'
            }
        ]
        
        for order_data in sample_orders:
            order_id = f"ORD{self.order_counter}"
            self.order_counter += 1
            
            order = {
                'order_id': order_id,
                'user_id': order_data['user_id'],
                'cart_id': f"CART_{order_data['user_id']}",
                'items': order_data['items'],
                'subtotal': order_data['total_amount'] * 0.9,
                'tax': order_data['total_amount'] * 0.1,
                'shipping_cost': 0.0,
                'total_amount': order_data['total_amount'],
                'status': order_data['status'],
                'shipping_address': order_data['shipping_address'],
                'payment_method': 'CREDIT_CARD',
                'created_at': (datetime.datetime.now() - datetime.timedelta(days=7)).isoformat(),
                'updated_at': datetime.datetime.now().isoformat()
            }
            self.orders[order_id] = order
            
        logger.info(f"Initialized with {len(sample_orders)} sample orders")
    
    def setup_rabbitmq(self):
        """Setup RabbitMQ connection for Saga pattern"""
        try:
            time.sleep(10)
            self.rabbitmq_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    port=5672,
                    credentials=pika.PlainCredentials('guest', 'guest')
                )
            )
            self.channel = self.rabbitmq_connection.channel()
            
            # Declare exchange for order events
            self.channel.exchange_declare(
                exchange='order_events',
                exchange_type='topic',
                durable=True
            )
            
            # Declare queue for payment results
            self.channel.queue_declare(queue='payment_results', durable=True)
            
            # Start consuming payment results
            self.start_consuming_payment_results()
            
            logger.info("✅ RabbitMQ setup completed for Saga pattern")
        except Exception as e:
            logger.error(f"❌ Failed to setup RabbitMQ: {str(e)}")
    
    def start_consuming_payment_results(self):
        """Start consuming payment result events"""
        def consume():
            channel = self.rabbitmq_connection.channel()
            
            def payment_result_callback(ch, method, properties, body):
                """Handle payment result events"""
                try:
                    event = json.loads(body)
                    logger.info(f"📥 Received payment result: {event['event_type']} for order {event['order_id']}")
                    
                    # Handle payment result
                    if event['event_type'] == 'payment.success':
                        self.handle_payment_success(event)
                    elif event['event_type'] == 'payment.failed':
                        self.handle_payment_failure(event)
                    
                except Exception as e:
                    logger.error(f"Error processing payment result: {str(e)}")
            
            channel.basic_consume(
                queue='payment_results',
                on_message_callback=payment_result_callback,
                auto_ack=True
            )
            
            logger.info("👂 Listening for payment results...")
            channel.start_consuming()
        
        # Start consumer in background thread
        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
    
    def handle_payment_success(self, event_data):
        """Handle successful payment"""
        try:
            order_id = event_data['order_id']
            
            if order_id in self.orders:
                order = self.orders[order_id]
                order['status'] = 'PROCESSING'
                order['updated_at'] = datetime.datetime.now().isoformat()
                
                logger.info(f"✅ Payment successful for order {order_id}, status updated to PROCESSING")
                
                # Send order.processing event
                self.send_order_event(
                    order_id=order_id,
                    user_id=order['user_id'],
                    event_type='order.processing',
                    status='PROCESSING'
                )
                
        except Exception as e:
            logger.error(f"Error handling payment success: {str(e)}")
    
    def handle_payment_failure(self, event_data):
        """Handle payment failure - Saga compensation"""
        try:
            order_id = event_data['order_id']
            
            if order_id in self.orders:
                order = self.orders[order_id]
                order['status'] = 'CANCELLED'
                order['updated_at'] = datetime.datetime.now().isoformat()
                
                logger.info(f"❌ Payment failed for order {order_id}, compensating: status updated to CANCELLED")
                
                # Send order cancelled event for compensation
                self.send_order_event(
                    order_id=order_id,
                    user_id=order['user_id'],
                    event_type='order.cancelled',
                    status='CANCELLED',
                    reason='Payment failed'
                )
                
        except Exception as e:
            logger.error(f"Error handling payment failure: {str(e)}")
    
    def send_order_event(self, order_id, user_id, event_type, status, reason=None):
        """Send order event to RabbitMQ"""
        try:
            event = {
                'event_id': str(uuid.uuid4()),
                'event_type': event_type,
                'timestamp': datetime.datetime.now().isoformat(),
                'order_id': order_id,
                'user_id': user_id,
                'status': status,
                'reason': reason
            }
            
            self.channel.basic_publish(
                exchange='order_events',
                routing_key=event_type,
                body=json.dumps(event),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            logger.info(f"📤 Published {event_type} event for order {order_id}")
            
        except Exception as e:
            logger.error(f"Error sending order event: {str(e)}")

    def CreateOrder(self, request, context):
        logger.info(f"Creating order for user: {request.user_id}")
        
        try:
            order_id = f"ORD{self.order_counter}"
            self.order_counter += 1
            
            # Calculate totals
            subtotal = sum(item.total for item in request.items)
            tax = subtotal * 0.1  # 10% tax
            shipping_cost = 9.99 if subtotal < 100 else 0  # Free shipping over $100
            total_amount = subtotal + tax + shipping_cost
            
            order = {
                'order_id': order_id,
                'user_id': request.user_id,
                'cart_id': request.cart_id,
                'items': [
                    {
                        'product_id': item.product_id,
                        'product_name': item.product_name,
                        'price': item.price,
                        'quantity': item.quantity,
                        'total': item.total
                    } for item in request.items
                ],
                'subtotal': subtotal,
                'tax': tax,
                'shipping_cost': shipping_cost,
                'total_amount': total_amount,
                'status': 'PENDING',
                'shipping_address': request.shipping_address,
                'payment_method': request.payment_method,
                'created_at': datetime.datetime.now().isoformat(),
                'updated_at': datetime.datetime.now().isoformat()
            }
            
            self.orders[order_id] = order
            
            # Send order.created event for Saga
            self.send_order_event(
                order_id=order_id,
                user_id=request.user_id,
                event_type='order.created',
                status='PENDING'
            )
            
            logger.info(f"Order created: {order_id} for user: {request.user_id}")
            logger.info(f"📤 Order created event published for Saga")
            
            return self._order_to_proto(order)
            
        except Exception as e:
            logger.error(f"Failed to create order: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to create order: {str(e)}')
            return order_pb2.OrderResponse()

    def GetOrder(self, request, context):
        logger.info(f"Getting order: {request.order_id}")
        
        try:
            order = self.orders.get(request.order_id)
            if not order:
                logger.warning(f"Order not found: {request.order_id}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Order not found')
                return order_pb2.OrderResponse()
            
            return self._order_to_proto(order)
            
        except Exception as e:
            logger.error(f"Failed to get order: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to get order: {str(e)}')
            return order_pb2.OrderResponse()

    def ListOrders(self, request, context):
        logger.info(f"Listing orders for user: {request.user_id}")
        
        try:
            user_orders = [
                order for order in self.orders.values()
                if order['user_id'] == request.user_id
            ]
            
            if request.status:
                user_orders = [o for o in user_orders if o['status'] == request.status]
            
            # Pagination
            page = request.page if request.page > 0 else 1
            limit = request.limit if request.limit > 0 else 10
            start_idx = (page - 1) * limit
            
            paginated_orders = user_orders[start_idx:start_idx + limit]
            
            return order_pb2.ListOrdersResponse(
                orders=[self._order_to_proto(order) for order in paginated_orders],
                total=len(user_orders),
                page=page,
                limit=limit
            )
            
        except Exception as e:
            logger.error(f"Failed to list orders: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to list orders: {str(e)}')
            return order_pb2.ListOrdersResponse()

    def UpdateOrderStatus(self, request, context):
        logger.info(f"Updating order status: {request.order_id} -> {request.status}")
        
        try:
            order = self.orders.get(request.order_id)
            if not order:
                logger.warning(f"Order not found: {request.order_id}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Order not found')
                return order_pb2.OrderResponse()
            
            valid_statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED']
            if request.status not in valid_statuses:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f'Invalid status. Must be one of: {valid_statuses}')
                return order_pb2.OrderResponse()
            
            old_status = order['status']
            order['status'] = request.status
            order['updated_at'] = datetime.datetime.now().isoformat()
            
            # Send status change event
            event_type = f'order.{request.status.lower()}'
            self.send_order_event(
                order_id=request.order_id,
                user_id=order['user_id'],
                event_type=event_type,
                status=request.status
            )
            
            logger.info(f"Order status updated: {request.order_id} {old_status} -> {request.status}")
            return self._order_to_proto(order)
            
        except Exception as e:
            logger.error(f"Failed to update order status: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to update order status: {str(e)}')
            return order_pb2.OrderResponse()

    def CancelOrder(self, request, context):
        logger.info(f"Canceling order: {request.order_id}")
        
        try:
            order = self.orders.get(request.order_id)
            if not order:
                logger.warning(f"Order not found: {request.order_id}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Order not found')
                return order_pb2.OrderResponse()
            
            if order['status'] == 'DELIVERED':
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details('Cannot cancel delivered order')
                return order_pb2.OrderResponse()
            
            order['status'] = 'CANCELLED'
            order['updated_at'] = datetime.datetime.now().isoformat()
            
            # Send order cancelled event
            self.send_order_event(
                order_id=request.order_id,
                user_id=order['user_id'],
                event_type='order.cancelled',
                status='CANCELLED',
                reason=request.reason
            )
            
            logger.info(f"Order cancelled: {request.order_id}. Reason: {request.reason}")
            return self._order_to_proto(order)
            
        except Exception as e:
            logger.error(f"Failed to cancel order: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to cancel order: {str(e)}')
            return order_pb2.OrderResponse()

    def _order_to_proto(self, order):
        """Convert internal order dict to protobuf response"""
        return order_pb2.OrderResponse(
            order_id=order['order_id'],
            user_id=order['user_id'],
            cart_id=order['cart_id'],
            items=[
                order_pb2.OrderItem(
                    product_id=item['product_id'],
                    product_name=item['product_name'],
                    price=item['price'],
                    quantity=item['quantity'],
                    total=item['total']
                ) for item in order['items']
            ],
            subtotal=order['subtotal'],
            tax=order['tax'],
            shipping_cost=order['shipping_cost'],
            total_amount=order['total_amount'],
            status=order['status'],
            shipping_address=order['shipping_address'],
            payment_method=order['payment_method'],
            created_at=order['created_at'],
            updated_at=order['updated_at']
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_pb2_grpc.add_OrderServiceServicer_to_server(OrderService(), server)
    server.add_insecure_port('[::]:50054')
    server.start()
    logger.info("🚀 Order Service running on port 50054")
    logger.info(f"📊 Initialized with {len(OrderService().orders)} orders")
    logger.info("🔄 Saga pattern enabled with RabbitMQ")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()