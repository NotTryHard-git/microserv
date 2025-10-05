import grpc
from concurrent import futures
import logging
import jwt
import datetime
from hashlib import sha256
import uuid

import auth_pb2
import auth_pb2_grpc

class AuthService(auth_pb2_grpc.AuthServiceServicer):
    def __init__(self):
        self.users = {}
        self.JWT_SECRET = "your-secret-key"
        self.JWT_ALGORITHM = "HS256"
        self.JWT_EXPIRATION = 24 * 60 * 60

    def Register(self, request, context):
        user_id = str(uuid.uuid4())
        
        if any(user['email'] == request.email for user in self.users.values()):
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details('User already exists')
            return auth_pb2.AuthResponse()
        
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
        token = self._generate_token(user_id, request.role)
        
        return auth_pb2.AuthResponse(
            token=token,
            user_id=user_id,
            email=request.email,
            role=request.role
        )

    def Login(self, request, context):
        user = next((u for u in self.users.values() if u['email'] == request.email), None)
        
        if not user or user['password'] != sha256(request.password.encode()).hexdigest():
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Invalid credentials')
            return auth_pb2.AuthResponse()
        
        token = self._generate_token(user['id'], user['role'])
        
        return auth_pb2.AuthResponse(
            token=token,
            user_id=user['id'],
            email=user['email'],
            role=user['role']
        )

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
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    auth_pb2_grpc.add_AuthServiceServicer_to_server(AuthService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Auth Service running on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()