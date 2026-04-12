"""
ORM relationships - joins, subqueries, and aggregates.
Run: uv run examples/orm_relationships.py
"""

from sqlalchemy import ForeignKey, Integer, String, create_engine, func, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
)

from sqlalchemy_profiler import Profiler


class Base(DeclarativeBase):
    pass


class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50))
    products: Mapped[list["Product"]] = relationship(
        "Product", back_populates="category"
    )


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100))
    price: Mapped[int] = mapped_column(Integer)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    category: Mapped["Category"] = relationship("Category", back_populates="products")
    orders: Mapped[list["OrderItem"]] = relationship(
        "OrderItem", back_populates="product"
    )


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer: Mapped[str] = mapped_column(String(100))
    items: Mapped[list["OrderItem"]] = relationship("OrderItem", back_populates="order")


class OrderItem(Base):
    __tablename__ = "order_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    quantity: Mapped[int] = mapped_column(Integer)
    order: Mapped["Order"] = relationship("Order", back_populates="items")
    product: Mapped["Product"] = relationship("Product", back_populates="orders")


engine = create_engine("sqlite:///:memory:", echo=False)
Base.metadata.create_all(engine)

with Session(engine) as session:
    electronics = Category(name="Electronics")
    books = Category(name="Books")
    session.add_all([electronics, books])
    session.flush()

    p1 = Product(name="Laptop", price=1200, category_id=electronics.id)
    p2 = Product(name="Phone", price=800, category_id=electronics.id)
    p3 = Product(name="SQLAlchemy in Action", price=45, category_id=books.id)
    p4 = Product(name="Clean Code", price=35, category_id=books.id)
    session.add_all([p1, p2, p3, p4])
    session.flush()

    o1 = Order(customer="Alice")
    o2 = Order(customer="Bob")
    session.add_all([o1, o2])
    session.flush()

    session.add_all(
        [
            OrderItem(order_id=o1.id, product_id=p1.id, quantity=1),
            OrderItem(order_id=o1.id, product_id=p3.id, quantity=2),
            OrderItem(order_id=o2.id, product_id=p2.id, quantity=1),
            OrderItem(order_id=o2.id, product_id=p4.id, quantity=3),
        ]
    )
    session.commit()


with Profiler(engine) as prof:
    with Session(engine) as session:

        # Simple join
        rows = session.execute(
            select(Product.name, Category.name).join(
                Category, Product.category_id == Category.id
            )
        ).all()

        # Aggregate: total revenue per category
        revenue = session.execute(
            select(
                Category.name,
                func.sum(Product.price * OrderItem.quantity).label("revenue"),
            )
            .join(Product, Product.category_id == Category.id)
            .join(OrderItem, OrderItem.product_id == Product.id)
            .group_by(Category.name)
        ).all()
        print("Revenue per category:")
        for row in revenue:
            print(f"  {row[0]}: ${row[1]}")

        # Subquery: products with at least 1 order
        ordered_ids = select(OrderItem.product_id).distinct().scalar_subquery()
        ordered_products = session.execute(
            select(Product.name).where(Product.id.in_(ordered_ids))
        ).all()

        # Window-style: most expensive product per category
        for cat in session.query(Category).all():
            _ = (
                session.query(Product)
                .filter(Product.category_id == cat.id)
                .order_by(Product.price.desc())
                .first()
            )

prof.print_stats()
