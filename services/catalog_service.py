import grpc
from concurrent import futures
import logging
import uuid

import catalog_pb2
import catalog_pb2_grpc

class CatalogService(catalog_pb2_grpc.CatalogServiceServicer):
    def __init__(self):
        self.products = {}
        self.categories = {}
        
        # Add sample category
        category_id = '1'
        self.categories[category_id] = {'id': category_id, 'name': 'Electronics'}

    def CreateProduct(self, request, context):
        product_id = str(uuid.uuid4())
        
        if request.category_id not in self.categories:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Category not found')
            return catalog_pb2.ProductResponse()
        
        product = {
            'id': product_id,
            'name': request.name,
            'description': request.description,
            'price': request.price,
            'category_id': request.category_id,
            'quantity': request.quantity,
            'image_url': request.image_url
        }
        
        self.products[product_id] = product
        
        return catalog_pb2.ProductResponse(
            id=product['id'],
            name=product['name'],
            description=product['description'],
            price=product['price'],
            category_id=product['category_id'],
            quantity=product['quantity'],
            image_url=product['image_url']
        )

    def GetProduct(self, request, context):
        product = self.products.get(request.product_id)
        if not product:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Product not found')
            return catalog_pb2.ProductResponse()
        
        return catalog_pb2.ProductResponse(**product)

    def ListProducts(self, request, context):
        products = list(self.products.values())
        
        if request.category_id:
            products = [p for p in products if p['category_id'] == request.category_id]
        
        page = request.page if request.page > 0 else 1
        limit = request.limit if request.limit > 0 else 10
        start_idx = (page - 1) * limit
        
        paginated_products = products[start_idx:start_idx + limit]
        
        return catalog_pb2.ListProductsResponse(
            products=[catalog_pb2.ProductResponse(**p) for p in paginated_products],
            total=len(products),
            page=page,
            limit=limit
        )

    def CreateCategory(self, request, context):
        category_id = str(uuid.uuid4())
        category = {'id': category_id, 'name': request.name}
        self.categories[category_id] = category
        
        return catalog_pb2.CategoryResponse(**category)

    def ListCategories(self, request, context):
        categories = list(self.categories.values())
        return catalog_pb2.ListCategoriesResponse(
            categories=[catalog_pb2.CategoryResponse(**c) for c in categories]
        )

    def UpdateProduct(self, request, context):
        # Implementation for update
        pass

    def DeleteProduct(self, request, context):
        # Implementation for delete
        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    catalog_pb2_grpc.add_CatalogServiceServicer_to_server(CatalogService(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Catalog Service running on port 50052")
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()