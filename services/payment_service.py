import grpc
from concurrent import futures
import logging
import uuid
import datetime
import pika
import json
from typing import Dict
import threading
import time
import payment_pb2
import payment_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('PaymentService')

class PaymentService(payment_pb2_grpc.PaymentServiceServicer):
    def __init__(self):
        self.payments: Dict[str, dict] = {}
        self.rabbitmq_connection = None
        self.channel = None
        self.setup_rabbitmq()
        logger.info("Payment Service initialized")
        
        # Initialize with sample data
        self._initialize_sample_data()
    
    def _initialize_sample_data(self):
        """Initialize with sample payments"""
        sample_payments = [
            {
                'order_id': 'ORD1000',
                'user_id': 'sample_user_1',
                'amount': 1099.99,
                'status': 'SUCCESS',
                'payment_method': 'CREDIT_CARD'
            },
            {
                'order_id': 'ORD1001',
                'user_id': 'sample_user_2',
                'amount': 2199.99,
                'status': 'SUCCESS',
                'payment_method': 'PAYPAL'
            }
        ]
        
        for payment_data in sample_payments:
            payment_id = str(uuid.uuid4())
            payment = {
                'payment_id': payment_id,
                'order_id': payment_data['order_id'],
                'user_id': payment_data['user_id'],
                'amount': payment_data['amount'],
                'status': payment_data['status'],
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}",
                'payment_method': payment_data['payment_method'],
                'created_at': datetime.datetime.now().isoformat(),
                'updated_at': datetime.datetime.now().isoformat()
            }
            self.payments[payment_id] = payment
        
        logger.info(f"Initialized with {len(sample_payments)} sample payments")
    
    def setup_rabbitmq(self):
        """Setup RabbitMQ connection and queues"""
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
            
            # Declare exchanges
            self.channel.exchange_declare(
                exchange='order_events',
                exchange_type='topic',
                durable=True
            )
            
            self.channel.exchange_declare(
                exchange='payment_events',
                exchange_type='topic',
                durable=True
            )
            
            # Declare queues
            self.channel.queue_declare(queue='order_payments', durable=True)
            self.channel.queue_declare(queue='payment_results', durable=True)
            self.channel.queue_declare(queue='order_compensations', durable=True)
            
            # Bind queues
            self.channel.queue_bind(
                exchange='order_events',
                queue='order_payments',
                routing_key='order.created'
            )
            
            self.channel.queue_bind(
                exchange='payment_events',
                queue='payment_results',
                routing_key='payment.*'
            )
            
            self.channel.queue_bind(
                exchange='payment_events',
                queue='order_compensations',
                routing_key='payment.failed'
            )
            
            # Start consuming in background thread
            self.start_consuming()
            
            logger.info("✅ RabbitMQ setup completed")
        except Exception as e:
            logger.error(f"❌ Failed to setup RabbitMQ: {str(e)}")
    
    def start_consuming(self):
        """Start consuming messages from RabbitMQ"""
        def consume():
            channel = self.rabbitmq_connection.channel()
            
            def order_created_callback(ch, method, properties, body):
                """Handle order.created events"""
                try:
                    event = json.loads(body)
                    logger.info(f"📥 Received order created event: {event['order_id']}")
                    
                    # Process payment
                    self.process_payment_from_event(event)
                    
                except Exception as e:
                    logger.error(f"Error processing order created event: {str(e)}")
            
            channel.basic_consume(
                queue='order_payments',
                on_message_callback=order_created_callback,
                auto_ack=True
            )
            
            logger.info("👂 Listening for order.created events...")
            channel.start_consuming()
        
        # Start consumer in background thread
        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
    
    def process_payment_from_event(self, event_data):
        """Process payment from RabbitMQ event"""
        try:
            # Simulate payment processing
            payment_id = str(uuid.uuid4())
            
            # Simulate payment failure 10% of the time for testing
            import random
            payment_success = random.random() > 0.1
            
            payment = {
                'payment_id': payment_id,
                'order_id': event_data['order_id'],
                'user_id': event_data['user_id'],
                'amount': event_data['total_amount'],
                'status': 'SUCCESS' if payment_success else 'FAILED',
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}",
                'payment_method': event_data.get('payment_method', 'CREDIT_CARD'),
                'created_at': datetime.datetime.now().isoformat(),
                'updated_at': datetime.datetime.now().isoformat()
            }
            
            self.payments[payment_id] = payment
            
            # Send payment result event
            self.send_payment_event(
                payment_id=payment_id,
                order_id=event_data['order_id'],
                success=payment_success,
                amount=event_data['total_amount']
            )
            
            logger.info(f"💳 Payment processed: {payment_id}, Status: {payment['status']}")
            
        except Exception as e:
            logger.error(f"Error processing payment from event: {str(e)}")
    
    def send_payment_event(self, payment_id, order_id, success, amount):
        """Send payment result event to RabbitMQ"""
        try:
            event_type = 'payment.success' if success else 'payment.failed'
            
            event = {
                'event_id': str(uuid.uuid4()),
                'event_type': event_type,
                'timestamp': datetime.datetime.now().isoformat(),
                'payment_id': payment_id,
                'order_id': order_id,
                'amount': amount,
                'success': success
            }
            
            self.channel.basic_publish(
                exchange='payment_events',
                routing_key=event_type,
                body=json.dumps(event),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            logger.info(f"📤 Published {event_type} event for order {order_id}")
            
        except Exception as e:
            logger.error(f"Error sending payment event: {str(e)}")
    
    def ProcessPayment(self, request, context):
        logger.info(f"Processing payment for order: {request.order_id}, user: {request.user_id}")
        
        try:
            payment_id = str(uuid.uuid4())
            
            # Validate payment details (simplified)
            if not request.card_number or len(request.card_number.replace(" ", "")) < 16:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Invalid card number')
                return payment_pb2.PaymentResponse()
            
            # Simulate payment processing
            # In real application, integrate with payment gateway
            payment_success = True  # Simulate success
            
            payment = {
                'payment_id': payment_id,
                'order_id': request.order_id,
                'user_id': request.user_id,
                'amount': request.amount,
                'status': 'SUCCESS' if payment_success else 'FAILED',
                'transaction_id': f"TXN{uuid.uuid4().hex[:8].upper()}",
                'payment_method': request.payment_method,
                'created_at': datetime.datetime.now().isoformat(),
                'updated_at': datetime.datetime.now().isoformat()
            }
            
            self.payments[payment_id] = payment
            
            # Send notification via RabbitMQ
            self.send_payment_event(
                payment_id=payment_id,
                order_id=request.order_id,
                success=payment_success,
                amount=request.amount
            )
            
            logger.info(f"Payment processed: {payment_id} for order: {request.order_id}")
            
            return self._payment_to_proto(payment)
            
        except Exception as e:
            logger.error(f"Failed to process payment: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Payment processing failed: {str(e)}')
            return payment_pb2.PaymentResponse()
    
    def GetPayment(self, request, context):
        logger.info(f"Getting payment: {request.payment_id}")
        
        try:
            payment = self.payments.get(request.payment_id)
            if not payment:
                logger.warning(f"Payment not found: {request.payment_id}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Payment not found')
                return payment_pb2.PaymentResponse()
            
            return self._payment_to_proto(payment)
            
        except Exception as e:
            logger.error(f"Failed to get payment: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to get payment: {str(e)}')
            return payment_pb2.PaymentResponse()
    
    def RefundPayment(self, request, context):
        logger.info(f"Refunding payment: {request.payment_id}")
        
        try:
            payment = self.payments.get(request.payment_id)
            if not payment:
                logger.warning(f"Payment not found for refund: {request.payment_id}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Payment not found')
                return payment_pb2.PaymentResponse()
            
            if payment['status'] != 'SUCCESS':
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(f"Cannot refund payment with status: {payment['status']}")
                return payment_pb2.PaymentResponse()
            
            # Process refund
            payment['status'] = 'REFUNDED'
            payment['updated_at'] = datetime.datetime.now().isoformat()
            
            logger.info(f"Payment refunded: {request.payment_id}. Reason: {request.reason}")
            
            return self._payment_to_proto(payment)
            
        except Exception as e:
            logger.error(f"Failed to refund payment: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to refund payment: {str(e)}')
            return payment_pb2.PaymentResponse()
    
    def _payment_to_proto(self, payment):
        """Convert internal payment dict to protobuf response"""
        return payment_pb2.PaymentResponse(
            payment_id=payment['payment_id'],
            order_id=payment['order_id'],
            user_id=payment['user_id'],
            amount=payment['amount'],
            status=payment['status'],
            transaction_id=payment['transaction_id'],
            payment_method=payment['payment_method'],
            created_at=payment['created_at'],
            updated_at=payment['updated_at']
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    server.add_insecure_port('[::]:50055')
    server.start()
    logger.info("🚀 Payment Service running on port 50055")
    logger.info(f"💰 Initialized with {len(PaymentService().payments)} payments")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()