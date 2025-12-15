from concurrent import futures
import grpc

import catalog_pb2
import catalog_pb2_grpc

import logging
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('CatalogService')

class CatalogService(catalog_pb2_grpc.CatalogServicer):

    def __init__(self):
        logger.info('Initializing Catalog Service...')

        self.products = {}
        # products[product_id] = {name, desc, price, category_id, quantity}
        self.categories = {}
        # categories[category_id] = name
        
        
        logger.info("Catalog Service initialized successfully")
    
    def GetAllProducts(self, request, context):
        logger.info('Getting All Products...')
        if not self.products:
            logger.warning("There are no products")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There are no products")
            return catalog_pb2.ProductsResponse()
        return catalog_pb2.ProductsResponse(
            products = [catalog_pb2.ProductResponse(product_id=product_id, **product) for product_id, product in self.products.items()]
        )
    
    def GetAllCategories(self, request, context):
        logger.info('Getting All Categories...')
        if not self.categories:
            logger.warning("There are no categories")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There are no categories")
            return catalog_pb2.CategoriesResponse()
        return catalog_pb2.CategoriesResponse(
            categories = [catalog_pb2.CategoryResponse(category_id=category_id, name=self.categories[category_id]) for category_id in self.categories]
        )
    
    def SearchProducts(self, request, context):
        logger.info('Searching Products...')
        search = request.search_request
        products = []

        for product_id, product in self.products.items():
            if search in product['name'] or search in product['desc'] or\
            search in self.categories[product['category_id']]:
                products.append(catalog_pb2.ProductResponse(product_id=product_id, **product))
        
        if not products:
            logger.warning("There are no products")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There are no products")
            return catalog_pb2.ProductsResponse()

        return catalog_pb2.ProductsResponse(
            products = products
        )
    
    def CreateProduct(self, request, context):
        logger.info('Creating Product...')
        product_id = str(uuid.uuid4())
        self.products[product_id] = {
            'name': request.name,
            'desc': request.desc,
            'price': request.price,
            'category_id': request.category_id,
            'quantity': request.quantity
        }
        return catalog_pb2.ProductResponse(product_id=product_id, **self.products[product_id])
    
    def CreateCategory(self, request, context):
        logger.info('Creating Category...')

        if any(category['name'] == request.name for category in self.categories.values()):
            logger.warning("There is already category with this name")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details("There is already category with this name")
            return catalog_pb2.CategoryResponse()
        
        category_id = str(uuid.uuid4())
        self.categories[category_id] = {'name': request.name}

        return catalog_pb2.CategoryResponse(category_id=category_id, name=self.categories[category_id]['name'])
    
    def UpdateProduct(self, request, context):
        logger.info('Updating Product...')
        if request.product_id not in self.products:
            logger.warning("There isn't product with this product_id")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't product with this product_id")
            return catalog_pb2.ProductResponse()
        
        self.products[request.product_id] = {
            'name': request.name,
            'desc': request.desc,
            'price': request.price,
            'category_id': request.category_id,
            'quantity': request.quantity
        }
        return catalog_pb2.ProductResponse(product_id=request.product_id, **self.products[request.product_id])
    
    def UpdateCategory(self, request, context):
        logger.info('Updating Category...')

        if request.category_id not in self.categories:
            logger.warning("There isn't category with this category_id")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't category with this category_id")
            return catalog_pb2.CategoryResponse()
        
        self.categories[request.category_id]['name'] = request.name

        return catalog_pb2.CategoryResponse(category_id=request.category_id, name=self.categories[request.category_id]['name']) 

    def GetProduct(self, request, context):
        logger.info('Getting Product...')

        if request.product_id not in self.products:
            logger.warning("There isn't product with this product_id")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't product with this product_id")
            return catalog_pb2.ProductResponse()
        
        return catalog_pb2.ProductResponse(product_id=request.product_id, **self.products[request.product_id])
    
    def GetCategory(self, request, context):
        logger.info('Getting Category...')

        if request.category_id not in self.categories:
            logger.warning("There isn't category with this category_id")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't category with this category_id")
            return catalog_pb2.CategoryResponse()

        return catalog_pb2.CategoryResponse(category_id=request.category_id, name=self.categories[request.category_id]['name']) 

    def DeleteProduct(self, request, context):
        logger.info('Deleting Product...')

        if request.product_id not in self.products:
            logger.warning("There isn't product with this product_id")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't product with this product_id")
            return catalog_pb2.DeleteResponse(success=False)
        
        del self.products[request.product_id]

        return catalog_pb2.DeleteResponse(success=True)
    
    def DeleteCategory(self, request, context):
        logger.info('Deleting Category...')

        if request.category_id not in self.categories:
            logger.warning("There isn't category with this category_id")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("There isn't category with this category_id")
            return catalog_pb2.DeleteResponse(success=False)
        
        del self.categories[request.category_id]

        return catalog_pb2.DeleteResponse(success=True)
        

def serve():
    logger.info('Starting Catalog Service...')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    catalog_pb2_grpc.add_CatalogServicer_to_server(CatalogService(), server)
    server.add_insecure_port("[::]:50052")
    logger.info('Catalog Service successfully started on port 50052.')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    try:
        serve()
    except KeyboardInterrupt:
        logger.info('Catalog Service stopped by user.')
    except Exception as e:
        logger.error(f'Catalog Service crashed: {str(e)}')