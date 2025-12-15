from concurrent import futures
import grpc

import auth_pb2
import auth_pb2_grpc

import logging
import jwt
import uuid
from hashlib import sha256
import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('AuthService')

class AuthService(auth_pb2_grpc.AuthServicer):

    def __init__(self):
        logger.info('Initializing Auth Service...')
        self.users = {}
        # users[uid] = {first_name, second_name, email, password, adress, is_admin}
        self.JWT_SECRET = "your-secret-key"
        self.JWT_ALGORITHM = "HS256"
        self.JWT_EXPIRATION = 24 * 60 * 60
        
        # Cart service client
        #self.cart_channel = grpc.insecure_channel('cart-service:50053')
        #self.cart_stub = cart_pb2_grpc.CartServiceStub(self.cart_channel)
        
        logger.info("Auth Service initialized successfully")
    
    def SignUp(self, request, context):
        try:

            if any(user['email'] == request.email for user in self.users.values()):
                logger.warning(f"User already exists: {request.email}")
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details('User already exists')
                return auth_pb2.AuthResponse()
            
            uid = str(uuid.uuid4())
            self.users[uid] = {
                'first_name': request.first_name,
                'second_name': request.second_name,
                'email': request.email,
                'password': sha256(request.password.encode()).hexdigest(),
                'adress': request.adress,
                'is_admin': request.is_admin
            }
            
            logger.info(f"User signed up successfully: {request.email} with ID: {uid}")

            token = self._generate_token(uid)
            logger.info(f"Token successfully generated for {request.email}")

            return auth_pb2.AuthResponse(
                token = token,
                uid = uid,
                is_admin = request.is_admin
            )

        except Exception as e:
            logger.error(f'Signing Up failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Signing Up failed: {str(e)}')
            return auth_pb2.AuthResponse()
    
    def SignIn(self, request, context):
        try:

            uid_to_find = ''
            for uid in self.users:
                if self.users[uid]['email'] == request.email:
                    uid_to_find = uid
                    break
            if not uid_to_find or self.users[uid_to_find]['password'] != sha256(request.password.encode()).hexdigest():
                logger.warning(f"Failed login attempt for: {request.email}")
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details('Invalid credentials')
                return auth_pb2.AuthResponse()
            
            logger.info(f"User signed up successfully: {request.email} with ID: {uid}")

            token = self._generate_token(uid_to_find)
            logger.info(f"Token successfully generated for {request.email}")

            return auth_pb2.AuthResponse(
                token = token,
                uid = uid_to_find,
                is_admin = self.users[uid_to_find]['is_admin']
            )
            
        except Exception as e:
            logger.error(f'Signing In failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Signing In failed: {str(e)}')
            return auth_pb2.AuthResponse()
    
    def ValidateToken(self, request, context):
        try:
            logger.info(f"Getting User...")
            payload = jwt.decode(request.token, self.JWT_SECRET, algorithms=[self.JWT_ALGORITHM])
            return auth_pb2.ValidateTokenResponse(
                valid = True,
                uid = payload['user_id']
            )
        
        except jwt.ExpiredSignatureError as e:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Token expired')
            logger.error(f'Validating Token is failed: {str(e)}')
        except jwt.InvalidTokenError as e:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Invalid token')
            logger.error(f'Validating Token is failed: {str(e)}')
        
        return auth_pb2.ValidateResponse(valid=False)
    
    def GetUser(self, request, context):
        try:

            if request.uid not in self.users:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('User not found')
                logger.warning(f'User {request.uid} is not found')
                return auth_pb2.GetUserResponse()
            
            user = self.users[request.uid]

            return auth_pb2.GetUserResponse(
                uid = request.uid,
                first_name = user['first_name'],
                second_name = user['second_name'],
                email = user['email'],
                adress = user['adress'],
                is_admin = user['is_admin']
            )

        except Exception as e:
            logger.error(f'Getting User is failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Getting User is failed: {str(e)}')
            return auth_pb2.GetUserResponse()
        
    def GetUsers(self, request, context):
        users = []
        
        for uid in self.users:
            users.append(auth_pb2.GetUserResponse(
                uid = uid,
                first_name=self.users[uid]['first_name'],
                second_name=self.users[uid]['second_name'],
                email=self.users[uid]['email'],
                adress=self.users[uid]['adress'],
                is_admin=self.users[uid]['is_admin']
            ))
        
        return auth_pb2.GetUsersResponse(users=users)
    
    def _generate_token(self, uid):
        payload = {
            'user_id': uid,
            'exp': datetime.datetime.now() + datetime.timedelta(seconds=self.JWT_EXPIRATION),
            'iat': datetime.datetime.now()
        }
        return jwt.encode(payload, self.JWT_SECRET, algorithm=self.JWT_ALGORITHM)

def serve():
    logger.info('Starting Auth Service...')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    auth_pb2_grpc.add_AuthServicer_to_server(AuthService(), server)
    server.add_insecure_port("[::]:50051")
    logger.info('Auth Service successfully started on port 50051.')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    try:
        serve()
    except KeyboardInterrupt:
        logger.info('Auth Service stopped by user.')
    except Exception as e:
        logger.error(f'Auth Service crashed: {str(e)}')