import grpc
import json
import uuid
import time

import order_pb2
import order_pb2_grpc
import auth_pb2
import auth_pb2_grpc

def test_saga_scenario():
    print("🔧 Тестирование Saga паттерна...")
    
    # 1. Создаем пользователя (или используем существующего)
    auth_channel = grpc.insecure_channel('localhost:50051')
    auth_stub = auth_pb2_grpc.AuthServiceStub(auth_channel)
    
    # Регистрируем тестового пользователя
    try:
        auth_response = auth_stub.Register(auth_pb2.RegisterRequest(
            email=f"test_{uuid.uuid4().hex[:8]}@example.com",
            password="test123",
            phone_number="+79991234567",
            role="CUSTOMER",
            bank_account="1234567890"
        ))
        user_id = auth_response.user_id
        print(f"✅ Создан пользователь: {user_id}")
    except:
        # Используем существующего
        user_id = "sample_user_1"
        print(f"✅ Используем существующего пользователя: {user_id}")
    
    # 2. Подключаемся к Order Service
    order_channel = grpc.insecure_channel('localhost:50053')
    order_stub = order_pb2_grpc.OrderServiceStub(order_channel)
    
    # 3. Добавляем товар в корзину
    print("🛒 Добавляем товар в корзину...")
    try:
        # Используем существующий товар
        product_id = "prod_001"
        
        cart_response = order_stub.AddToCart(order_pb2.AddToCartRequest(
            user_id=user_id,
            product_id=product_id,
            quantity=1
        ))
        
        if cart_response.success:
            cart_product_id = cart_response.cart_product_id
            print(f"✅ Товар добавлен в корзину: {cart_product_id}")
        else:
            print("❌ Не удалось добавить товар в корзину")
            return
    except Exception as e:
        print(f"❌ Ошибка добавления в корзину: {str(e)}")
        return
    
    # 4. Покупаем товар (запускаем Saga)
    print("💰 Покупаем товар (запуск Saga)...")
    try:
        buy_response = order_stub.BuyFromCart(order_pb2.BuyFromCartRequest(
            user_id=user_id,
            cart_product_id=cart_product_id,
            bank_details="1234567890"
        ))
        
        if buy_response.success:
            order_id = buy_response.order_id
            print(f"✅ Заказ создан: {order_id}")
            print("🔄 Saga запущена: Order → RabbitMQ → Payment → Notification")
        else:
            print("❌ Не удалось создать заказ")
            return
    except Exception as e:
        print(f"❌ Ошибка создания заказа: {str(e)}")
        return
    
    # 5. Проверяем статус заказа через несколько секунд
    print("\n⏳ Ждем обработки платежа (5 секунд)...")
    time.sleep(5)
    
    try:
        order_response = order_stub.GetOrder(order_pb2.GetOrderRequest(
            order_id=order_id
        ))
        
        print(f"\n📦 Статус заказа {order_id}: {order_response.status}")
        
        if order_response.status == 'CONFIRMED':
            print("✅ Saga успешно завершена!")
            print("   - Заказ создан")
            print("   - Платеж успешно обработан")
            print("   - Уведомления отправлены")
            print("   - Товар списан со склада")
        elif order_response.status == 'FAILED':
            print("🔄 Saga выполнена с компенсацией!")
            print("   - Заказ создан")
            print("   - Платеж не прошел")
            print("   - Товар возвращен в корзину")
            print("   - Уведомления отправлены")
        else:
            print("⏳ Saga все еще обрабатывается...")
            
    except Exception as e:
        print(f"❌ Ошибка проверки статуса: {str(e)}")
    
    # 6. Проверяем RabbitMQ
    print("\n📊 Проверьте RabbitMQ:")
    print("   http://localhost:15672")
    print("   Логин: guest, Пароль: guest")
    print("   Очередь 'payment_queue' должна содержать сообщения")

if __name__ == '__main__':
    test_saga_scenario()