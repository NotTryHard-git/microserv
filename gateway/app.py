from flask import Flask, request, jsonify
import grpc
import auth_pb2
import auth_pb2_grpc
import catalog_pb2
import catalog_pb2_grpc

app = Flask(__name__)

SERVICE_CONFIG = {
    'auth': 'auth-service:50051',
    'catalog': 'catalog-service:50052'
}

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
                'role': response.role
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

auth_client = AuthClient()
catalog_client = CatalogClient()

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)