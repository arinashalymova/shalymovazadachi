import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from app.models import Customer, Order, OrderItem, Product

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderItemData:
    product_id: int
    quantity: int


def create_order(session: Session, customer_id: int, items: list[OrderItemData]) -> int:
    if len(items) == 0:
        raise ValueError("В заказе должна быть минимум одна позиция")

    log.info("Начинается создание заказа для клиента с ID=%s", customer_id)

    try:
        with session.begin():
            cust = session.get(Customer, customer_id)
            if cust is None:
                raise ValueError(f"Клиент с идентификатором {customer_id} не существует")

            new_order = Order(customer_id=customer_id, order_date=datetime.utcnow(), total_amount=Decimal("0.00"))
            session.add(new_order)
            session.flush()

            for item_data in items:
                if item_data.quantity <= 0:
                    raise ValueError(f"Количество товара {item_data.product_id} должно быть положительным числом")

                prod = session.get(Product, item_data.product_id)
                if prod is None:
                    raise ValueError(f"Продукт с идентификатором {item_data.product_id} не найден")

                item_subtotal = Decimal(prod.price) * item_data.quantity
                new_item = OrderItem(
                    order_id=new_order.order_id,
                    product_id=item_data.product_id,
                    quantity=item_data.quantity,
                    subtotal=item_subtotal,
                )
                session.add(new_item)
                log.info(
                    "Добавлен товар в заказ: order_id=%s, product_id=%s, qty=%s, subtotal=%s",
                    new_order.order_id,
                    item_data.product_id,
                    item_data.quantity,
                    item_subtotal,
                )

            session.flush()

            order_total = session.scalar(
                select(func.coalesce(func.sum(OrderItem.subtotal), 0)).where(OrderItem.order_id == new_order.order_id)
            )
            new_order.total_amount = Decimal(order_total)

        log.info("Заказ успешно создан с ID=%s, итоговая сумма=%s", new_order.order_id, new_order.total_amount)
        return new_order.order_id
    except Exception as e:
        session.rollback()
        log.exception("Ошибка при создании заказа, откат транзакции: %s", e)
        raise


def change_customer_email(session: Session, customer_id: int, new_email: str) -> None:
    log.info("Обновление email для клиента ID=%s", customer_id)

    try:
        with session.begin():
            if not new_email or "@" not in new_email:
                raise ValueError("Email имеет некорректный формат")

            duplicate = session.scalar(select(Customer).where(Customer.email == new_email))
            if duplicate is not None and duplicate.customer_id != customer_id:
                raise ValueError(f"Email {new_email} уже зарегистрирован в системе")

            cust = session.get(Customer, customer_id)
            if cust is None:
                raise ValueError(f"Клиент с идентификатором {customer_id} не найден")

            cust.email = new_email

        log.info("Email клиента ID=%s успешно изменен", customer_id)
    except (ValueError, IntegrityError, SQLAlchemyError) as e:
        session.rollback()
        log.exception("Ошибка при обновлении email, откат транзакции: %s", e)
        raise


def insert_product(session: Session, product_name: str, price: Decimal) -> int:
    log.info("Добавление нового продукта: name=%s, price=%s", product_name, price)

    try:
        with session.begin():
            if not product_name or not product_name.strip():
                raise ValueError("Имя продукта не может быть пустым")
            if price <= 0:
                raise ValueError("Цена продукта должна быть положительной")

            new_product = Product(product_name=product_name.strip(), price=price)
            session.add(new_product)
            session.flush()

        log.info("Продукт добавлен с ID=%s", new_product.product_id)
        return new_product.product_id
    except IntegrityError as e:
        session.rollback()
        if "products_productname_key" in str(e.orig):
            raise ValueError(f"Продукт '{product_name}' уже присутствует в базе данных") from e
        log.exception("Ошибка целостности при добавлении продукта, откат: %s", e)
        raise
    except (ValueError, IntegrityError, SQLAlchemyError) as e:
        session.rollback()
        log.exception("Ошибка при добавлении продукта, откат транзакции: %s", e)
        raise
