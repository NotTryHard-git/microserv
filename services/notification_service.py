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
import notification_pb2
import notification_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('NotificationService')

class NotificationService(notification_pb2_grpc.NotificationServiceServicer):
    def __init__(self):
        self.notifications: Dict[str, List[dict]] = {}
        self.rabbitmq_connection = None
        self.channel = None
        self.setup_rabbitmq()
        logger.info("Notification Service initialized")
    
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
            
            self.channel.exchange_declare(
                exchange='notification_events',
                exchange_type='topic',
                durable=True
            )
            
            # Declare notification queue
            self.channel.queue_declare(queue='all_notifications', durable=True)
            
            # Bind to all events
            self.channel.queue_bind(
                exchange='order_events',
                queue='all_notifications',
                routing_key='order.*'
            )
            
            self.channel.queue_bind(
                exchange='payment_events',
                queue='all_notifications',
                routing_key='payment.*'
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
            
            def notification_callback(ch, method, properties, body):
                """Handle all events and send notifications"""
                try:
                    event = json.loads(body)
                    logger.info(f"📥 Received event: {event['event_type']}")
                    
                    # Process event and send notification
                    self.process_event_and_notify(event)
                    
                except Exception as e:
                    logger.error(f"Error processing event: {str(e)}")
            
            channel.basic_consume(
                queue='all_notifications',
                on_message_callback=notification_callback,
                auto_ack=True
            )
            
            logger.info("👂 Listening for all events...")
            channel.start_consuming()
        
        # Start consumer in background thread
        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
    
    def process_event_and_notify(self, event_data):
        """Process event and send appropriate notification"""
        try:
            event_type = event_data.get('event_type', '')
            user_id = event_data.get('user_id')
            
            if not user_id:
                # Try to extract user_id from event data
                if 'order_id' in event_data:
                    # In real app, we'd fetch user_id from order service
                    user_id = 'unknown_user'
            
            notification_type = self._map_event_to_notification_type(event_type)
            title, message = self._create_notification_content(event_type, event_data)
            
            # Store notification
            self._store_notification(user_id, notification_type, title, message, event_data)
            
            # Send notification (simulated)
            self._send_notification(user_id, notification_type, title, message)
            
            logger.info(f"📧 Notification sent for event: {event_type}")
            
        except Exception as e:
            logger.error(f"Error processing event for notification: {str(e)}")
    
    def _map_event_to_notification_type(self, event_type):
        """Map event type to notification type"""
        mapping = {
            'order.created': 'ORDER_CREATED',
            'payment.success': 'PAYMENT_SUCCESS',
            'payment.failed': 'PAYMENT_FAILED',
            'order.cancelled': 'ORDER_CANCELLED',
            'order.shipped': 'ORDER_SHIPPED',
            'order.delivered': 'ORDER_DELIVERED'
        }
        return mapping.get(event_type, 'SYSTEM_NOTIFICATION')
    
    def _create_notification_content(self, event_type, event_data):
        """Create title and message based on event type"""
        if event_type == 'order.created':
            return (
                "🎉 Order Created Successfully!",
                f"Your order #{event_data.get('order_id')} has been created and is being processed."
            )
        elif event_type == 'payment.success':
            return (
                "✅ Payment Successful!",
                f"Payment for order #{event_data.get('order_id')} has been processed successfully."
            )
        elif event_type == 'payment.failed':
            return (
                "❌ Payment Failed",
                f"Payment for order #{event_data.get('order_id')} failed. Please try again."
            )
        elif event_type == 'order.cancelled':
            return (
                "📦 Order Cancelled",
                f"Your order #{event_data.get('order_id')} has been cancelled."
            )
        else:
            return (
                "System Notification",
                f"Event: {event_type}"
            )
    
    def _store_notification(self, user_id, notification_type, title, message, metadata):
        """Store notification in memory"""
        if user_id not in self.notifications:
            self.notifications[user_id] = []
        
        notification = {
            'notification_id': str(uuid.uuid4()),
            'user_id': user_id,
            'type': notification_type,
            'title': title,
            'message': message,
            'metadata': metadata,
            'sent': True,
            'created_at': datetime.datetime.now().isoformat()
        }
        
        self.notifications[user_id].append(notification)
        
        # Keep only last 100 notifications per user
        if len(self.notifications[user_id]) > 100:
            self.notifications[user_id] = self.notifications[user_id][-100:]
    
    def _send_notification(self, user_id, notification_type, title, message):
        """Simulate sending notification"""
        # In real application, this would:
        # 1. Send email
        # 2. Send SMS
        # 3. Send push notification
        # 4. Store in database
        
        logger.info(f"✉️ Sending {notification_type} to user {user_id}: {title}")
    
    def SendNotification(self, request, context):
        logger.info(f"Sending notification to user: {request.user_id}, type: {request.type}")
        
        try:
            notification_id = str(uuid.uuid4())
            
            if request.user_id not in self.notifications:
                self.notifications[request.user_id] = []
            
            notification = {
                'notification_id': notification_id,
                'user_id': request.user_id,
                'type': request.type,
                'title': request.title,
                'message': request.message,
                'metadata': dict(request.metadata),
                'sent': True,
                'created_at': datetime.datetime.now().isoformat()
            }
            
            self.notifications[request.user_id].append(notification)
            
            logger.info(f"Notification sent: {notification_id} to user: {request.user_id}")
            
            return notification_pb2.NotificationResponse(
                notification_id=notification_id,
                user_id=request.user_id,
                type=request.type,
                title=request.title,
                message=request.message,
                sent=True,
                created_at=notification['created_at']
            )
            
        except Exception as e:
            logger.error(f"Failed to send notification: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to send notification: {str(e)}')
            return notification_pb2.NotificationResponse()
    
    def GetUserNotifications(self, request, context):
        logger.info(f"Getting notifications for user: {request.user_id}")
        
        try:
            user_notifications = self.notifications.get(request.user_id, [])
            
            # Apply pagination
            limit = request.limit if request.limit > 0 else 10
            offset = request.offset if request.offset >= 0 else 0
            
            paginated = user_notifications[offset:offset + limit]
            
            return notification_pb2.UserNotificationsResponse(
                notifications=[
                    notification_pb2.NotificationResponse(
                        notification_id=n['notification_id'],
                        user_id=n['user_id'],
                        type=n['type'],
                        title=n['title'],
                        message=n['message'],
                        sent=n['sent'],
                        created_at=n['created_at']
                    ) for n in paginated
                ],
                total=len(user_notifications)
            )
            
        except Exception as e:
            logger.error(f"Failed to get notifications: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to get notifications: {str(e)}')
            return notification_pb2.UserNotificationsResponse()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notification_pb2_grpc.add_NotificationServiceServicer_to_server(
        NotificationService(), server
    )
    server.add_insecure_port('[::]:50056')
    server.start()
    logger.info("🚀 Notification Service running on port 50056")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()