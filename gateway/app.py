from flask import Flask, request, jsonify, g
import grpc
import auth_pb2
import auth_pb2_grpc
import catalog_pb2
import catalog_pb2_grpc
import cart_pb2
import cart_pb2_grpc
import order_pb2
import order_pb2_grpc
import notification_pb2
import notification_pb2_grpc
import payment_pb2
import payment_pb2_grpc

app = Flask(__name__)

SERVICE_CONFIG = {
    'auth': 'auth-service:50051',
    'catalog': 'catalog-service:50052',
    'cart': 'cart-service:50053',
    'order': 'order-service:50054',  # NEW SERVICE
    'payment': 'payment-service:50055',  # NEW SERVICE
    'notification': 'notification-service:50056'  # NEW SERVICE
}
# Middleware для проверки токена
def validate_token(token):
    try:
        channel = grpc.insecure_channel(SERVICE_CONFIG['auth'])
        stub = auth_pb2_grpc.AuthServiceStub(channel)
        response = stub.ValidateToken(auth_pb2.ValidateRequest(token=token))
        return response.valid, response.user_id, response.role
    except Exception as e:
        return False, None, None

@app.before_request
def before_request():
    # Пропускаем публичные эндпоинты
    public_endpoints = ['/api/auth/register', '/api/auth/login', '/health', '/api/products']
    if request.path in public_endpoints:
        return
    
    # Проверяем токен для защищенных эндпоинтов
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Missing or invalid Authorization header'}), 401
    
    token = auth_header.split(' ')[1]
    valid, user_id, role = validate_token(token)
    
    if not valid:
        return jsonify({'error': 'Invalid or expired token'}), 401
    
    g.user_id = user_id
    g.user_role = role


class AuthClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['auth'])
        self.stub = auth_pb2_grpc.AuthServiceStub(self.channel)
    
    def register(self, data):
        try:
            response = self.stub.Register(auth_pb2.RegisterRequest(
                email=data['email'],
                phone_number=data.get('phone_number', ''),
                password=data['password'],
                role=data.get('role', 'CUSTOMER'),
                bank_account=data.get('bank_account', '')
            ))
            return {
                'token': response.token,
                'user_id': response.user_id,
                'email': response.email,
                'role': response.role,
                'cart_id': response.cart_id  # NEW FIELD!
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    def login(self, data):
        try:
            response = self.stub.Login(auth_pb2.LoginRequest(
                email=data['email'],
                password=data['password']
            ))
            return {
                'token': response.token,
                'user_id': response.user_id,
                'email': response.email,
                'role': response.role
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

class CartClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['cart'])
        self.stub = cart_pb2_grpc.CartServiceStub(self.channel)
    
    def get_cart(self, user_id):
        try:
            response = self.stub.GetCart(cart_pb2.GetCartRequest(user_id=user_id))
            return {
                'cart_id': response.cart_id,
                'user_id': response.user_id,
                'items': [{
                    'product_id': item.product_id,
                    'product_name': item.product_name,
                    'price': item.price,
                    'quantity': item.quantity,
                    'total': item.total
                } for item in response.items],
                'total_amount': response.total_amount,
                'total_items': response.total_items,
                'created_at': response.created_at,
                'updated_at': response.updated_at
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def add_to_cart(self, user_id, product_id, quantity):
        try:
            response = self.stub.AddItem(cart_pb2.AddItemRequest(
                user_id=user_id,
                product_id=product_id,
                quantity=quantity
            ))
            return {
                'cart_id': response.cart_id,
                'user_id': response.user_id,
                'items': [{
                    'product_id': item.product_id,
                    'product_name': item.product_name,
                    'price': item.price,
                    'quantity': item.quantity,
                    'total': item.total
                } for item in response.items],
                'total_amount': response.total_amount,
                'total_items': response.total_items
            }
        except grpc.RpcError as e:
            return {'error': e.details()}


class CatalogClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['catalog'])
        self.stub = catalog_pb2_grpc.CatalogServiceStub(self.channel)
    
    def create_product(self, data):
        try:
            response = self.stub.CreateProduct(catalog_pb2.CreateProductRequest(
                name=data['name'],
                description=data.get('description', ''),
                price=data['price'],
                category_id=data['category_id'],
                quantity=data.get('quantity', 0),
                image_url=data.get('image_url', '')
            ))
            return self._product_to_dict(response)
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def list_products(self, category_id=None, page=1, limit=10):
        try:
            response = self.stub.ListProducts(catalog_pb2.ListProductsRequest(
                category_id=category_id or '',
                page=page,
                limit=limit
            ))
            return {
                'products': [self._product_to_dict(p) for p in response.products],
                'total': response.total,
                'page': response.page,
                'limit': response.limit
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def _product_to_dict(self, product):
        return {
            'id': product.id,
            'name': product.name,
            'description': product.description,
            'price': product.price,
            'category_id': product.category_id,
            'quantity': product.quantity,
            'image_url': product.image_url
        }

class OrderClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['order'])
        self.stub = order_pb2_grpc.OrderServiceStub(self.channel)
    
    def create_order(self, user_id, cart_id, shipping_address, payment_method, items):
        try:
            response = self.stub.CreateOrder(order_pb2.CreateOrderRequest(
                user_id=user_id,
                cart_id=cart_id,
                shipping_address=shipping_address,
                payment_method=payment_method,
                items=items
            ))
            return self._order_to_dict(response)
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def get_order(self, order_id):
        try:
            response = self.stub.GetOrder(order_pb2.GetOrderRequest(order_id=order_id))
            return self._order_to_dict(response)
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def list_orders(self, user_id, status=None, page=1, limit=10):
        try:
            response = self.stub.ListOrders(order_pb2.ListOrdersRequest(
                user_id=user_id,
                status=status or '',
                page=page,
                limit=limit
            ))
            return {
                'orders': [self._order_to_dict(order) for order in response.orders],
                'total': response.total,
                'page': response.page,
                'limit': response.limit
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def _order_to_dict(self, order):
        return {
            'order_id': order.order_id,
            'user_id': order.user_id,
            'cart_id': order.cart_id,
            'items': [
                {
                    'product_id': item.product_id,
                    'product_name': item.product_name,
                    'price': item.price,
                    'quantity': item.quantity,
                    'total': item.total
                } for item in order.items
            ],
            'subtotal': order.subtotal,
            'tax': order.tax,
            'shipping_cost': order.shipping_cost,
            'total_amount': order.total_amount,
            'status': order.status,
            'shipping_address': order.shipping_address,
            'payment_method': order.payment_method,
            'created_at': order.created_at,
            'updated_at': order.updated_at
        }
class PaymentClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['payment'])
        self.stub = payment_pb2_grpc.PaymentServiceStub(self.channel)
    
    def process_payment(self, data):
        try:
            response = self.stub.ProcessPayment(payment_pb2.PaymentRequest(
                order_id=data['order_id'],
                user_id=data['user_id'],
                amount=data['amount'],
                payment_method=data.get('payment_method', 'CREDIT_CARD'),
                card_number=data.get('card_number', ''),
                card_expiry=data.get('card_expiry', ''),
                card_cvv=data.get('card_cvv', '')
            ))
            return {
                'payment_id': response.payment_id,
                'order_id': response.order_id,
                'status': response.status,
                'transaction_id': response.transaction_id,
                'amount': response.amount
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def get_payment(self, payment_id):
        try:
            response = self.stub.GetPayment(payment_pb2.PaymentIdRequest(payment_id=payment_id))
            return self._payment_to_dict(response)
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def _payment_to_dict(self, payment):
        return {
            'payment_id': payment.payment_id,
            'order_id': payment.order_id,
            'user_id': payment.user_id,
            'amount': payment.amount,
            'status': payment.status,
            'transaction_id': payment.transaction_id,
            'payment_method': payment.payment_method,
            'created_at': payment.created_at,
            'updated_at': payment.updated_at
        }
    def get_circuit_breaker_state(self):
        try:
            response = self.stub.GetCircuitBreakerState(payment_pb2.Empty())
            return {
                'state': response.state,
                'state_code': response.state_code,
                'failure_count': response.failure_count,
                'request_count': response.request_count,
                'failure_rate': response.failure_rate,
                'last_state_change': response.last_state_change
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def set_service_mode(self, mode):
        try:
            response = self.stub.SetServiceMode(payment_pb2.ServiceModeRequest(mode=mode))
            return {
                'success': response.success,
                'old_mode': response.old_mode,
                'new_mode': response.new_mode
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def reset_circuit_breaker(self):
        try:
            response = self.stub.ResetCircuitBreaker(payment_pb2.Empty())
            return {
                'success': response.success,
                'message': response.message
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

class NotificationClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['notification'])
        self.stub = notification_pb2_grpc.NotificationServiceStub(self.channel)
    
    def send_notification(self, data):
        try:
            response = self.stub.SendNotification(notification_pb2.NotificationRequest(
                user_id=data['user_id'],
                type=data['type'],
                title=data['title'],
                message=data['message'],
                metadata=data.get('metadata', {})
            ))
            return {
                'notification_id': response.notification_id,
                'sent': response.sent
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def get_notifications(self, user_id, limit=10, offset=0):
        try:
            response = self.stub.GetUserNotifications(
                notification_pb2.UserNotificationsRequest(
                    user_id=user_id,
                    limit=limit,
                    offset=offset
                )
            )
            return {
                'notifications': [
                    {
                        'notification_id': n.notification_id,
                        'type': n.type,
                        'title': n.title,
                        'message': n.message,
                        'created_at': n.created_at
                    } for n in response.notifications
                ],
                'total': response.total
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

auth_client = AuthClient()
catalog_client = CatalogClient()
cart_client = CartClient()
order_client = OrderClient()  # NEW CLIENT
payment_client = PaymentClient()  # NEW CLIENT
notification_client = NotificationClient()  # NEW CLIENT

# Auth routes

@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.get_json()
    result = auth_client.register(data)
    return jsonify(result)

@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    result = auth_client.login(data)
    return jsonify(result)

# Cart routes
@app.route('/api/cart/<user_id>', methods=['GET'])
def get_cart(user_id):
    result = cart_client.get_cart(user_id)
    return jsonify(result)

@app.route('/api/cart/<user_id>/add', methods=['POST'])
def add_to_cart(user_id):
    data = request.get_json()
    product_id = data.get('product_id')
    quantity = data.get('quantity', 1)
    
    result = cart_client.add_to_cart(user_id, product_id, quantity)
    return jsonify(result)

# Catalog routers
@app.route('/api/products', methods=['POST'])
def create_product():
    data = request.get_json()
    result = catalog_client.create_product(data)
    return jsonify(result)

@app.route('/api/products', methods=['GET'])
def list_products():
    category_id = request.args.get('category_id')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    
    result = catalog_client.list_products(category_id, page, limit)
    return jsonify(result)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'API Gateway is running'})


# Order routes
@app.route('/api/orders', methods=['POST'])
def create_order():
    data = request.get_json()
    
    # Получаем корзину пользователя
    cart_result = cart_client.get_cart(g.user_id)
    if 'error' in cart_result:
        return jsonify({'error': 'Cart not found'}), 404
    
    # Преобразуем items из корзины в формат для заказа
    order_items = [
        order_pb2.OrderItem(
            product_id=item['product_id'],
            product_name=item['product_name'],
            price=item['price'],
            quantity=item['quantity'],
            total=item['total']
        ) for item in cart_result['items']
    ]
    
    # Создаем заказ
    result = order_client.create_order(
        user_id=g.user_id,
        cart_id=cart_result['cart_id'],
        shipping_address=data.get('shipping_address'),
        payment_method=data.get('payment_method', 'CREDIT_CARD'),
        items=order_items
    )
    
    return jsonify(result)

@app.route('/api/orders/<order_id>', methods=['GET'])
def get_order(order_id):
    result = order_client.get_order(order_id)
    if 'error' in result:
        return jsonify(result), 404
    return jsonify(result)

@app.route('/api/orders', methods=['GET'])
def list_orders():
    status = request.args.get('status')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    
    result = order_client.list_orders(g.user_id, status, page, limit)
    return jsonify(result)

# Добавим еще продуктов в каталог
@app.route('/api/catalog/seed', methods=['POST'])
def seed_catalog():
    """Seed the catalog with sample products"""
    sample_products = [
        {
            'name': 'iPhone 13 Pro',
            'description': 'Latest Apple smartphone',
            'price': 999.99,
            'category_id': '1',
            'quantity': 50
        },
        {
            'name': 'MacBook Pro 16"',
            'description': 'Powerful laptop for professionals',
            'price': 2499.99,
            'category_id': '1',
            'quantity': 30
        },
        {
            'name': 'Samsung Galaxy S21',
            'description': 'Android flagship smartphone',
            'price': 799.99,
            'category_id': '1',
            'quantity': 100
        },
        {
            'name': 'Sony WH-1000XM4',
            'description': 'Noise cancelling headphones',
            'price': 349.99,
            'category_id': '1',
            'quantity': 75
        }
    ]
    
    results = []
    for product in sample_products:
        result = catalog_client.create_product(product)
        results.append(result)
    
    return jsonify({'created': len(results), 'products': results})

# Payment routes
@app.route('/api/payments/process', methods=['POST'])
def process_payment():
    data = request.get_json()
    
    result = payment_client.process_payment(data)
    return jsonify(result)

@app.route('/api/payments/<payment_id>', methods=['GET'])
def get_payment(payment_id):
    result = payment_client.get_payment(payment_id)
    if 'error' in result:
        return jsonify(result), 404
    return jsonify(result)

# Notification routes
@app.route('/api/notifications', methods=['POST'])
def send_notification():
    data = request.get_json()
    
    result = notification_client.send_notification(data)
    return jsonify(result)

@app.route('/api/notifications/user/<user_id>', methods=['GET'])
def get_user_notifications(user_id):
    limit = int(request.args.get('limit', 10))
    offset = int(request.args.get('offset', 0))
    
    result = notification_client.get_notifications(user_id, limit, offset)
    return jsonify(result)

# Saga Demo Route
@app.route('/api/saga/demo', methods=['POST'])
def saga_demo():
    """Демонстрация работы Saga паттерна"""
    data = request.get_json()
    user_id = g.user_id
    
    # 1. Получаем корзину пользователя
    cart_result = cart_client.get_cart(user_id)
    if 'error' in cart_result or len(cart_result.get('items', [])) == 0:
        return jsonify({'error': 'Cart is empty'}), 400
    
    # 2. Создаем заказ
    order_items = [
        order_pb2.OrderItem(
            product_id=item['product_id'],
            product_name=item['product_name'],
            price=item['price'],
            quantity=item['quantity'],
            total=item['total']
        ) for item in cart_result['items']
    ]
    
    order_result = order_client.create_order(
        user_id=user_id,
        cart_id=cart_result['cart_id'],
        shipping_address=data.get('shipping_address', 'Test Address'),
        payment_method=data.get('payment_method', 'CREDIT_CARD'),
        items=order_items
    )
    
    if 'error' in order_result:
        return jsonify({'error': 'Failed to create order'}), 500
    
    # 3. Обрабатываем платеж (в реальном приложении это делается через RabbitMQ)
    payment_data = {
        'order_id': order_result['order_id'],
        'user_id': user_id,
        'amount': order_result['total_amount'],
        'payment_method': data.get('payment_method', 'CREDIT_CARD'),
        'card_number': data.get('card_number', '4111111111111111'),
        'card_expiry': data.get('card_expiry', '12/25'),
        'card_cvv': data.get('card_cvv', '123')
    }
    
    payment_result = payment_client.process_payment(payment_data)
    
    if 'error' in payment_result:
        # Если платеж не прошел, отменяем заказ (компенсация)
        order_client.cancel_order(order_result['order_id'], 'Payment failed')
        return jsonify({
            'message': 'Saga failed: Payment processing failed, order cancelled',
            'order_id': order_result['order_id'],
            'order_status': 'CANCELLED',
            'payment_status': 'FAILED'
        }), 400
    
    return jsonify({
        'message': 'Saga completed successfully!',
        'order_id': order_result['order_id'],
        'order_status': 'PROCESSING',
        'payment_id': payment_result['payment_id'],
        'payment_status': payment_result['status']
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)