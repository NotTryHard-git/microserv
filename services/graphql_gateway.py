import grpc
import strawberry
from typing import List, Optional
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI, HTTPException
import logging
from contextlib import asynccontextmanager

# Импортируем сгенерированные gRPC модули
import catalog_pb2
import catalog_pb2_grpc
import order_pb2
import order_pb2_grpc

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GraphQLGateway')

# Конфигурация адресов микросервисов
SERVICE_CONFIG = {
    'catalog': 'catalog-service:50052',
    'order': 'order-service:50054',
}

# Глобальные gRPC каналы и стабы (клиенты)
catalog_channel = None
order_channel = None
catalog_stub = None
order_stub = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения: создание и закрытие gRPC соединений."""
    global catalog_channel, order_channel, catalog_stub, order_stub
    
    # Создаем соединения при запуске
    logger.info("Инициализация gRPC соединений с микросервисами...")
    try:
        catalog_channel = grpc.insecure_channel(SERVICE_CONFIG['catalog'])
        order_channel = grpc.insecure_channel(SERVICE_CONFIG['order'])
        
        # Создаем клиентов (стабы)
        catalog_stub = catalog_pb2_grpc.CatalogServiceStub(catalog_channel)
        order_stub = order_pb2_grpc.OrderServiceStub(order_channel)
        
        logger.info("✅ gRPC соединения установлены")
        yield
    except Exception as e:
        logger.error(f"❌ Ошибка при установке gRPC соединений: {e}")
        raise
    finally:
        # Закрываем соединения при остановке
        logger.info("Закрытие gRPC соединений...")
        if catalog_channel:
            catalog_channel.close()
        if order_channel:
            order_channel.close()
        logger.info("gRPC соединения закрыты")

# ==================== GraphQL ТИПЫ ====================

@strawberry.type
class Product:
    id: str
    name: str
    description: Optional[str] = None
    price: float
    category_id: str
    quantity: int
    image_url: Optional[str] = None

@strawberry.type
class OrderItem:
    product_id: str
    product_name: str
    price: float
    quantity: int
    total: float

@strawberry.type
class Order:
    order_id: str
    user_id: str
    cart_id: Optional[str] = None
    items: List[OrderItem]
    subtotal: float
    tax: float
    shipping_cost: float
    total_amount: float
    status: str
    shipping_address: str
    payment_method: str
    created_at: str
    updated_at: str

@strawberry.type
class User:
    id: str
    email: str
    role: str

@strawberry.type
class OrderResult:
    success: bool
    order_id: Optional[str] = None
    message: str

# ==================== GraphQL ЗАПРОСЫ (Queries) ====================

@strawberry.type
class Query:
    
    @strawberry.field
    async def product(self, id: str) -> Optional[Product]:
        """Получить продукт по ID"""
        try:
            logger.info(f"Запрос продукта с ID: {id}")
            response = catalog_stub.GetProduct(catalog_pb2.GetProductRequest(product_id=id))
            
            return Product(
                id=response.id,
                name=response.name,
                description=response.description,
                price=response.price,
                category_id=response.category_id,
                quantity=response.quantity,
                image_url=response.image_url
            )
        except grpc.RpcError as e:
            logger.error(f"Ошибка при запросе продукта {id}: {e.details()}")
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise HTTPException(status_code=500, detail=f"Catalog service error: {e.details()}")
    
    @strawberry.field
    async def products(
        self, 
        category_id: Optional[str] = None,
        page: Optional[int] = 1,
        limit: Optional[int] = 10
    ) -> List[Product]:
        """Получить список продуктов с фильтрацией и пагинацией"""
        try:
            logger.info(f"Запрос списка продуктов. Категория: {category_id}, Страница: {page}, Лимит: {limit}")
            
            response = catalog_stub.ListProducts(catalog_pb2.ListProductsRequest(
                category_id=category_id or "",
                page=page,
                limit=limit
            ))
            
            products_list = []
            for product in response.products:
                products_list.append(Product(
                    id=product.id,
                    name=product.name,
                    description=product.description,
                    price=product.price,
                    category_id=product.category_id,
                    quantity=product.quantity,
                    image_url=product.image_url
                ))
            
            logger.info(f"Получено {len(products_list)} продуктов")
            return products_list
            
        except grpc.RpcError as e:
            logger.error(f"Ошибка при запросе списка продуктов: {e.details()}")
            raise HTTPException(status_code=500, detail=f"Catalog service error: {e.details()}")
    
    @strawberry.field
    async def orders(self, user_id: str) -> List[Order]:
        """Получить все заказы пользователя"""
        try:
            logger.info(f"Запрос заказов для пользователя: {user_id}")
            
            response = order_stub.ListOrders(order_pb2.ListOrdersRequest(
                user_id=user_id,
                page=1,
                limit=100  # Достаточно большое число, чтобы получить все заказы
            ))
            
            orders_list = []
            for order in response.orders:
                # Преобразуем элементы заказа
                order_items = []
                for item in order.items:
                    order_items.append(OrderItem(
                        product_id=item.product_id,
                        product_name=item.product_name,
                        price=item.price,
                        quantity=item.quantity,
                        total=item.total
                    ))
                
                orders_list.append(Order(
                    order_id=order.order_id,
                    user_id=order.user_id,
                    cart_id=order.cart_id,
                    items=order_items,
                    subtotal=order.subtotal,
                    tax=order.tax,
                    shipping_cost=order.shipping_cost,
                    total_amount=order.total_amount,
                    status=order.status,
                    shipping_address=order.shipping_address,
                    payment_method=order.payment_method,
                    created_at=order.created_at,
                    updated_at=order.updated_at
                ))
            
            logger.info(f"Получено {len(orders_list)} заказов для пользователя {user_id}")
            return orders_list
            
        except grpc.RpcError as e:
            logger.error(f"Ошибка при запросе заказов: {e.details()}")
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return []  # Если заказов нет, возвращаем пустой список
            raise HTTPException(status_code=500, detail=f"Order service error: {e.details()}")
    
    @strawberry.field
    async def user_orders_with_products(self, user_id: str) -> List[Order]:
        """
        Комбинированный запрос: заказы пользователя с полной информацией о продуктах.
        Демонстрирует мощь GraphQL для агрегации данных.
        """
        try:
            logger.info(f"Комбинированный запрос для пользователя: {user_id}")
            
            # 1. Получаем заказы пользователя
            orders_response = order_stub.ListOrders(order_pb2.ListOrdersRequest(
                user_id=user_id,
                page=1,
                limit=50
            ))
            
            orders_with_details = []
            
            # 2. Для каждого заказа обогащаем данные о продуктах
            for order in orders_response.orders:
                enriched_items = []
                
                for item in order.items:
                    # Запрашиваем детальную информацию о продукте из каталога
                    try:
                        product_detail = catalog_stub.GetProduct(
                            catalog_pb2.GetProductRequest(product_id=item.product_id)
                        )
                        
                        # Создаем обогащенный элемент заказа
                        enriched_items.append(OrderItem(
                            product_id=item.product_id,
                            product_name=product_detail.name,  # Берем имя из каталога
                            price=item.price,
                            quantity=item.quantity,
                            total=item.total
                        ))
                    except grpc.RpcError:
                        # Если не удалось получить детали продукта, используем базовую информацию
                        enriched_items.append(OrderItem(
                            product_id=item.product_id,
                            product_name=item.product_name,
                            price=item.price,
                            quantity=item.quantity,
                            total=item.total
                        ))
                
                # Создаем обогащенный заказ
                orders_with_details.append(Order(
                    order_id=order.order_id,
                    user_id=order.user_id,
                    cart_id=order.cart_id,
                    items=enriched_items,
                    subtotal=order.subtotal,
                    tax=order.tax,
                    shipping_cost=order.shipping_cost,
                    total_amount=order.total_amount,
                    status=order.status,
                    shipping_address=order.shipping_address,
                    payment_method=order.payment_method,
                    created_at=order.created_at,
                    updated_at=order.updated_at
                ))
            
            logger.info(f"Создано {len(orders_with_details)} обогащенных заказов")
            return orders_with_details
            
        except grpc.RpcError as e:
            logger.error(f"Ошибка в комбинированном запросе: {e.details()}")
            raise HTTPException(status_code=500, detail=f"Service error: {e.details()}")
    
    @strawberry.field
    async def health(self) -> str:
        """Проверка здоровья сервиса"""
        try:
            # Проверяем доступность каталога
            catalog_stub.ListProducts(catalog_pb2.ListProductsRequest(limit=1))
            
            # Проверяем доступность заказов
            order_stub.ListOrders(order_pb2.ListOrdersRequest(user_id="test", limit=1))
            
            return "✅ Все системы работают нормально"
        except grpc.RpcError as e:
            logger.warning(f"Проблемы с подключением к сервисам: {e.details()}")
            return f"⚠️ Проблемы с подключением: {e.details()}"
        except Exception as e:
            logger.error(f"Непредвиденная ошибка health check: {e}")
            return f"❌ Критическая ошибка: {str(e)}"

# ==================== GraphQL МУТАЦИИ (Mutations) ====================

@strawberry.type
class Mutation:
    
    @strawberry.mutation
    async def create_order(
        self, 
        user_id: str,
        product_id: str,
        quantity: int,
        shipping_address: str,
        payment_method: str = "CREDIT_CARD"
    ) -> OrderResult:
        """Создать новый заказ"""
        try:
            logger.info(f"Создание заказа для пользователя {user_id}, товар: {product_id}, количество: {quantity}")
            
            # 1. Получаем информацию о продукте из каталога
            product_response = catalog_stub.GetProduct(
                catalog_pb2.GetProductRequest(product_id=product_id)
            )
            
            # 2. Подготавливаем элемент заказа
            order_item = order_pb2.OrderItem(
                product_id=product_id,
                product_name=product_response.name,
                price=product_response.price,
                quantity=quantity,
                total=product_response.price * quantity
            )
            
            # 3. Создаем заказ
            order_response = order_stub.CreateOrder(order_pb2.CreateOrderRequest(
                user_id=user_id,
                cart_id=f"cart_{user_id}",  # В реальном приложении cart_id нужно получать из корзины
                shipping_address=shipping_address,
                payment_method=payment_method,
                items=[order_item]
            ))
            
            logger.info(f"Заказ создан успешно: {order_response.order_id}")
            
            return OrderResult(
                success=True,
                order_id=order_response.order_id,
                message=f"Заказ {order_response.order_id} успешно создан"
            )
            
        except grpc.RpcError as e:
            logger.error(f"Ошибка при создании заказа: {e.details()}")
            return OrderResult(
                success=False,
                message=f"Ошибка при создании заказа: {e.details()}"
            )

# ==================== НАСТРОЙКА И ЗАПУСК ====================

# Создаем GraphQL схему
schema = strawberry.Schema(query=Query, mutation=Mutation)

# Создаем FastAPI приложение
graphql_app = GraphQLRouter(schema, path="/graphql")

app = FastAPI(
    title="GraphQL Gateway для микросервисов магазина",
    description="Агрегирует данные из Catalog и Order сервисов",
    version="1.0.0",
    lifespan=lifespan
)

# Подключаем GraphQL роутер
app.include_router(graphql_app)

# Добавляем REST эндпоинты для обратной совместимости
@app.get("/")
async def root():
    """Корневой эндпоинт с информацией о сервисе"""
    return {
        "service": "GraphQL Gateway",
        "version": "1.0.0",
        "description": "Агрегатор данных для микросервисов магазина",
        "endpoints": {
            "graphql": "/graphql",
            "health": "/health",
            "openapi": "/docs"
        },
        "connected_services": list(SERVICE_CONFIG.keys())
    }

@app.get("/health")
async def health_check():
    """REST эндпоинт для проверки здоровья"""
    try:
        # Быстрая проверка сервисов
        catalog_stub.ListProducts(catalog_pb2.ListProductsRequest(limit=1))
        return {"status": "healthy", "service": "GraphQL Gateway"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}, 503

@app.get("/api/products")
async def rest_products(category_id: str = None, page: int = 1, limit: int = 10):
    """REST эндпоинт для получения продуктов (для обратной совместимости)"""
    products = await Query().products(category_id, page, limit)
    return {"products": products, "total": len(products)}

# Точка входа для запуска
if __name__ == "__main__":
    import uvicorn
    logger.info("=" * 60)
    logger.info("🚀 Запуск GraphQL Gateway на Strawberry...")
    logger.info(f"📡 Подключение к сервисам: {SERVICE_CONFIG}")
    logger.info("📍 GraphQL Playground: http://localhost:8081/graphql")
    logger.info("📍 REST Health Check: http://localhost:8081/health")
    logger.info("=" * 60)
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8081,
        log_level="info"
    )