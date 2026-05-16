from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Numeric, DateTime, ForeignKey, UniqueConstraint
from decimal import Decimal
from datetime import datetime


class BaseModel(DeclarativeBase):
    pass


class Customer(BaseModel):
    __tablename__ = "customers"

    customer_id: Mapped[int] = mapped_column("customerid", primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column("email", String(255), nullable=False, unique=True)
    first_name: Mapped[str] = mapped_column("firstname", String(100), nullable=False)
    last_name: Mapped[str] = mapped_column("lastname", String(100), nullable=False)

    orders: Mapped[list["Order"]] = relationship(back_populates="customer")


class Product(BaseModel):
    __tablename__ = "products"
    __table_args__ = (UniqueConstraint("productname", name="uq_products_productname"),)

    product_id: Mapped[int] = mapped_column("productid", primary_key=True, autoincrement=True)
    product_name: Mapped[str] = mapped_column("productname", String(255), nullable=False)
    price: Mapped[Decimal] = mapped_column("price", Numeric(10, 2), nullable=False)

    order_items: Mapped[list["OrderItem"]] = relationship(back_populates="product")


class Order(BaseModel):
    __tablename__ = "orders"

    order_id: Mapped[int] = mapped_column("orderid", primary_key=True, autoincrement=True)
    customer_id: Mapped[int] = mapped_column(
        "customerid",
        ForeignKey("customers.customerid", ondelete="RESTRICT"),
        nullable=False,
    )
    total_amount: Mapped[Decimal] = mapped_column("totalamount", Numeric(10, 2), nullable=False, default=Decimal("0.00"))
    order_date: Mapped[datetime] = mapped_column("orderdate", DateTime, nullable=False, default=datetime.utcnow)

    customer: Mapped["Customer"] = relationship(back_populates="orders")
    items: Mapped[list["OrderItem"]] = relationship(back_populates="order", cascade="all, delete-orphan")


class OrderItem(BaseModel):
    __tablename__ = "orderitems"

    order_item_id: Mapped[int] = mapped_column("orderitemid", primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        "orderid",
        ForeignKey("orders.orderid", ondelete="CASCADE"),
        nullable=False,
    )
    product_id: Mapped[int] = mapped_column(
        "productid",
        ForeignKey("products.productid", ondelete="RESTRICT"),
        nullable=False,
    )
    quantity: Mapped[int] = mapped_column("quantity", nullable=False)
    subtotal: Mapped[Decimal] = mapped_column("subtotal", Numeric(10, 2), nullable=False)

    order: Mapped["Order"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship(back_populates="order_items")
