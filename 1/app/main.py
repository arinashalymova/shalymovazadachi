import logging
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.config import app_config
from app.database import get_db_session, check_database_connection
from app.transactions import OrderItemData, create_order, change_customer_email, insert_product


def setup_logging() -> None:
    logging.basicConfig(
        level=app_config.logging_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def display_table_data(title: str, query: str) -> None:
    with get_db_session() as session:
        result = session.execute(text(query)).mappings().all()
    print(f"\n{title}")
    for row in result:
        print(dict(row))


def execute_scenarios() -> None:
    log = logging.getLogger(__name__)

    with get_db_session() as session:
        order_id = create_order(
            session=session,
            customer_id=1,
            items=[OrderItemData(product_id=1, quantity=2), OrderItemData(product_id=2, quantity=1)],
        )
        log.info("Сценарий 1 выполнен. ID заказа: %s", order_id)

    display_table_data(
        "Таблица заказов после сценария 1",
        "SELECT orderid, customerid, orderdate, totalamount FROM orders ORDER BY orderid",
    )
    display_table_data(
        "Позиции заказов после сценария 1",
        "SELECT orderitemid, orderid, productid, quantity, subtotal FROM orderitems ORDER BY orderitemid",
    )

    with get_db_session() as session:
        change_customer_email(session=session, customer_id=1, new_email="updated.alex@example.com")
        log.info("Сценарий 2 выполнен")

    display_table_data(
        "Таблица клиентов после сценария 2",
        "SELECT customerid, firstname, lastname, email FROM customers ORDER BY customerid",
    )

    with get_db_session() as session:
        product_id = insert_product(session=session, product_name="Mechanical Keyboard", price=Decimal("149.99"))
        log.info("Сценарий 3 выполнен. ID продукта: %s", product_id)

    display_table_data(
        "Таблица продуктов после сценария 3",
        "SELECT productid, productname, price FROM products ORDER BY productid",
    )


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    try:
        check_database_connection()
        execute_scenarios()
        log.info("Все сценарии завершены успешно")
    except SQLAlchemyError:
        log.exception("Произошла ошибка базы данных")
        raise
    except Exception:
        log.exception("Непредвиденная ошибка в процессе выполнения")
        raise


if __name__ == "__main__":
    main()
