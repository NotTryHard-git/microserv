import grpc
from concurrent import futures
import logging
import uuid
import datetime
from typing import Dict, List

import cart_pb2
import cart_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('CartService')

class CartService(cart_pb2_grpc.CartServiceServicer):
    def __init__(self):
        self.carts: Dict[str, dict] = {}  # user_id -> cart_data
        logger.info("Cart Service initialized")

    def CreateCart(self, request, context):
        logger.info(f"Creating cart for user: {request.user_id}")
        
        try:
            # Check if cart already exists
            if request.user_id in self.carts:
                logger.warning(f"Cart already exists for user: {request.user_id}")
                cart = self.carts[request.user_id]
                return cart_pb2.CartResponse(
                    cart_id=cart['cart_id'],
                    user_id=cart['user_id'],
                    items=[self._item_to_proto(item) for item in cart['items']],
                    total_amount=cart['total_amount'],
                    total_items=cart['total_items'],
                    created_at=cart['created_at'],
                    updated_at=cart['updated_at']
                )
            
            # Create new cart
            cart_id = str(uuid.uuid4())
            cart_data = {
                'cart_id': cart_id,
                'user_id': request.user_id,
                'items': [],
                'total_amount': 0.0,
                'total_items': 0,
                'created_at': datetime.datetime.now().isoformat(),
                'updated_at': datetime.datetime.now().isoformat()
            }
            
            self.carts[request.user_id] = cart_data
            logger.info(f"Cart created successfully: {cart_id} for user: {request.user_id}")
            
            return cart_pb2.CartResponse(
                cart_id=cart_data['cart_id'],
                user_id=cart_data['user_id'],
                items=[],
                total_amount=cart_data['total_amount'],
                total_items=cart_data['total_items'],
                created_at=cart_data['created_at'],
                updated_at=cart_data['updated_at']
            )
            
        except Exception as e:
            logger.error(f"Failed to create cart for user {request.user_id}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to create cart: {str(e)}')
            return cart_pb2.CartResponse()

    def GetCart(self, request, context):
        logger.info(f"Getting cart for user: {request.user_id}")
        
        try:
            cart = self.carts.get(request.user_id)
            if not cart:
                logger.warning(f"Cart not found for user: {request.user_id}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Cart not found')
                return cart_pb2.CartResponse()
            
            return cart_pb2.CartResponse(
                cart_id=cart['cart_id'],
                user_id=cart['user_id'],
                items=[self._item_to_proto(item) for item in cart['items']],
                total_amount=cart['total_amount'],
                total_items=cart['total_items'],
                created_at=cart['created_at'],
                updated_at=cart['updated_at']
            )
            
        except Exception as e:
            logger.error(f"Failed to get cart for user {request.user_id}: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to get cart: {str(e)}')
            return cart_pb2.CartResponse()

    def AddItem(self, request, context):
        logger.info(f"Adding item to cart: user={request.user_id}, product={request.product_id}, quantity={request.quantity}")
        
        try:
            cart = self.carts.get(request.user_id)
            if not cart:
                logger.warning(f"Cart not found for user: {request.user_id}")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Cart not found')
                return cart_pb2.CartResponse()
            
            # In a real application, we would fetch product details from Catalog Service
            # For now, we'll use mock product data
            product_price = 10.0  # Mock price
            product_name = f"Product {request.product_id}"  # Mock name
            
            # Check if item already exists in cart
            existing_item = next((item for item in cart['items'] if item['product_id'] == request.product_id), None)
            
            if existing_item:
                # Update quantity
                existing_item['quantity'] += request.quantity
                existing_item['total'] = existing_item['quantity'] * product_price
            else:
                # Add new item
                new_item = {
                    'product_id': request.product_id,
                    'product_name': product_name,
                    'price': product_price,
                    'quantity': request.quantity,
                    'total': request.quantity * product_price
                }
                cart['items'].append(new_item)
            
            # Update cart totals
            self._update_cart_totals(cart)
            cart['updated_at'] = datetime.datetime.now().isoformat()
            
            logger.info(f"Item added to cart: {request.product_id} for user: {request.user_id}")
            
            return cart_pb2.CartResponse(
                cart_id=cart['cart_id'],
                user_id=cart['user_id'],
                items=[self._item_to_proto(item) for item in cart['items']],
                total_amount=cart['total_amount'],
                total_items=cart['total_items'],
                created_at=cart['created_at'],
                updated_at=cart['updated_at']
            )
            
        except Exception as e:
            logger.error(f"Failed to add item to cart: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to add item: {str(e)}')
            return cart_pb2.CartResponse()

    def _update_cart_totals(self, cart):
        """Update total amount and total items count"""
        total_amount = 0.0
        total_items = 0
        
        for item in cart['items']:
            total_amount += item['total']
            total_items += item['quantity']
        
        cart['total_amount'] = total_amount
        cart['total_items'] = total_items

    def _item_to_proto(self, item):
        return cart_pb2.CartItem(
            product_id=item['product_id'],
            product_name=item['product_name'],
            price=item['price'],
            quantity=item['quantity'],
            total=item['total']
        )

    def RemoveItem(self, request, context):
        # Implementation for removing items
        pass

    def ClearCart(self, request, context):
        # Implementation for clearing cart
        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cart_pb2_grpc.add_CartServiceServicer_to_server(CartService(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    logger.info("🚀 Cart Service running on port 50053")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()