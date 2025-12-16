from concurrent import futures
import grpc

import notification_pb2
import notification_pb2_grpc

import logging
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('NotificationService')

class NotificationService(notification_pb2_grpc.NotificationServicer):

    def __init__(self):
        logger.info('Initializing Notification Service...')

        self.notifications = {}
        # notifications[notification_id] = {notification_id, uid, order_id, status}
        self.user_notifications = {}
        # user_notifications[uid] = [notification_id]
        
        
        logger.info("Notification Service initialized successfully")
    
    def GetNotification(self, request, context):
        logger.info('Getting Notification...')
        
        if request.notification_id not in self.notifications:
            logger.warning("There isn't notification with this id.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't notification with this id.")
            return notification_pb2.NotificationResponse()
        
        return notification_pb2.NotificationResponse(**self.notifications[request.notification_id])
    
    def GetUserNotifications(self, request, context):
        logger.info("Getting User Notifications...")

        if request.uid not in self.user_notifications:
            # Мб такой пользователь есть, просто у него нет уведомлений
            return notification_pb2.NotificationsResponse(notifications=[])
        
        notifications = []

        for notification_id in self.user_notifications[request.uid]:
            notifications.append(
                notification_pb2.NotificationResponse(**self.notifications[notification_id])
                )
        return notification_pb2.NotificationsResponse(notifications=notifications)
    
    def CreateNotification(self, request, context): # без проверки uid, тк он уже проверен в Order при создании заказа
        logger.info("Creating Notification")

        notification_id = str(uuid.uuid4())
        
        self.notifications[notification_id] = {
            'notification_id': notification_id,
            'uid': request.uid,
            'order_id': request.order_id,
            'status': request.status
        }

        if request.uid not in self.user_notifications:
            self.user_notifications[request.uid] = []
        
        self.user_notifications[request.uid].append(notification_id)

        return notification_pb2.SuccessResponse(success=True)

    def DeleteNotification(self, request, context):
        logger.info("Deleting Notification")

        if request.notification_id not in self.notifications:
            logger.warning("There isn't notification with this id.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't notification with this id.")
            return notification_pb2.SuccessResponse(success=False)

        uid = self.notifications.pop(request.notification_id)['uid']

        self.user_notifications[uid].remove(request.notification_id)

        return notification_pb2.SuccessResponse(success=True)
    
        

def serve():
    logger.info('Starting Notification Service...')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notification_pb2_grpc.add_NotificationServicer_to_server(NotificationService(), server)
    server.add_insecure_port("[::]:50055")
    logger.info('Notification Service successfully started on port 50055.')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    try:
        serve()
    except KeyboardInterrupt:
        logger.info('Notification Service stopped by user.')
    except Exception as e:
        logger.error(f'Notification Service crashed: {str(e)}')