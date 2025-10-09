import grpc
from concurrent import futures
import logging
import jwt
import datetime
from hashlib import sha256
import uuid

import auth_pb2
import auth_pb2_grpc
import cart_pb2
import cart_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('AuthService')

class AuthService(auth_pb2_grpc.AuthServiceServicer):
    def __init__(self):
        logger.info("Initializing Auth Service...")
        self.users = {}
        self.JWT_SECRET = "your-secret-key"
        self.JWT_ALGORITHM = "HS256"
        self.JWT_EXPIRATION = 24 * 60 * 60
        
        # Cart service client
        self.cart_channel = grpc.insecure_channel('cart-service:50053')
        self.cart_stub = cart_pb2_grpc.CartServiceStub(self.cart_channel)
        
        logger.info("Auth Service initialized successfully")

    def Register(self, request, context):
        logger.info(f"Register request received for email: {request.email}")
        
        try:
            user_id = str(uuid.uuid4())
            
            # Check if user already exists
            if any(user['email'] == request.email for user in self.users.values()):
                logger.warning(f"User already exists: {request.email}")
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details('User already exists')
                return auth_pb2.AuthResponse()
            
            # Create user data
            user_data = {
                'id': user_id,
                'email': request.email,
                'phone_number': request.phone_number,
                'password': sha256(request.password.encode()).hexdigest(),
                'role': request.role,
                'bank_account': request.bank_account,
                'created_at': datetime.datetime.now().isoformat()
            }
            
            self.users[user_id] = user_data
            logger.info(f"User registered successfully: {request.email} with ID: {user_id}")
            
            # Create cart for the user
            cart_response = self._create_user_cart(user_id)
            if not cart_response:
                logger.error(f"Failed to create cart for user: {user_id}")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details('Failed to create user cart')
                return auth_pb2.AuthResponse()
            
            logger.info(f"Cart created successfully for user: {user_id}, cart_id: {cart_response.cart_id}")
            
            # Generate JWT token
            token = self._generate_token(user_id, request.role)
            logger.debug(f"JWT token generated for user: {user_id}")
            
            # Return response with cart_id
            return auth_pb2.AuthResponse(
                token=token,
                user_id=user_id,
                email=request.email,
                role=request.role,
                cart_id=cart_response.cart_id  # New field!
            )
            
        except Exception as e:
            logger.error(f"Registration failed for {request.email}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Registration failed: {str(e)}')
            return auth_pb2.AuthResponse()

    def _create_user_cart(self, user_id):
        """Create a cart for the newly registered user"""
        try:
            cart_request = cart_pb2.CreateCartRequest(user_id=user_id)
            cart_response = self.cart_stub.CreateCart(cart_request)
            return cart_response
        except Exception as e:
            logger.error(f"Failed to create cart via Cart Service: {str(e)}")
            return None

    def Login(self, request, context):
        logger.info(f"Login attempt for email: {request.email}")
        user = next((u for u in self.users.values() if u['email'] == request.email), None)
        
        if not user or user['password'] != sha256(request.password.encode()).hexdigest():
            logger.warning(f"Failed login attempt for: {request.email}")
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Invalid credentials')
            return auth_pb2.AuthResponse()
        
        # Get user's cart
        cart_response = self._get_user_cart(user['id'])
        cart_id = cart_response.cart_id if cart_response else ""
        
        token = self._generate_token(user['id'], user['role'])
        logger.info(f"Successful login for: {request.email}")
        
        return auth_pb2.AuthResponse(
            token=token,
            user_id=user['id'],
            email=user['email'],
            role=user['role'],
            cart_id=cart_id
        )

    def _get_user_cart(self, user_id):
        """Get user's cart"""
        try:
            cart_request = cart_pb2.GetCartRequest(user_id=user_id)
            cart_response = self.cart_stub.GetCart(cart_request)
            return cart_response
        except Exception as e:
            logger.error(f"Failed to get cart for user {user_id}: {str(e)}")
            return None

    def ValidateToken(self, request, context):
        try:
            payload = jwt.decode(request.token, self.JWT_SECRET, algorithms=[self.JWT_ALGORITHM])
            return auth_pb2.ValidateResponse(
                valid=True,
                user_id=payload['user_id'],
                role=payload['role']
            )
        except jwt.ExpiredSignatureError:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Token expired')
        except jwt.InvalidTokenError:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Invalid token')
        
        return auth_pb2.ValidateResponse(valid=False)

    def GetUserProfile(self, request, context):
        user = self.users.get(request.user_id)
        if not user:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('User not found')
            return auth_pb2.UserProfile()
        
        return auth_pb2.UserProfile(
            id=user['id'],
            email=user['email'],
            phone_number=user['phone_number'],
            role=user['role'],
            bank_account=user['bank_account'],
            created_at=user['created_at']
        )

    def _generate_token(self, user_id, role):
        payload = {
            'user_id': user_id,
            'role': role,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.JWT_EXPIRATION),
            'iat': datetime.datetime.utcnow()
        }
        return jwt.encode(payload, self.JWT_SECRET, algorithm=self.JWT_ALGORITHM)

def serve():
    logger.info("Starting Auth Service gRPC server...")
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        auth_pb2_grpc.add_AuthServiceServicer_to_server(AuthService(), server)
        server.add_insecure_port('[::]:50051')
        server.start()
        logger.info("✅ Auth Service successfully started on port 50051")
        logger.info("🚀 Ready to accept requests")
        server.wait_for_termination()
    except Exception as e:
        logger.error(f"❌ Failed to start Auth Service: {str(e)}")
        raise

if __name__ == '__main__':
    try:
        serve()
    except KeyboardInterrupt:
        logger.info("Auth Service stopped by user")
    except Exception as e:
        logger.error(f"Auth Service crashed: {str(e)}")