"""
Basic example - raw SQL and simple ORM operations.
Run: uv run examples/basic.py
"""

from sqlalchemy import Integer, String, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from sqlalchemy_spy import Profiler


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50))
    email: Mapped[str] = mapped_column(String(100))


engine = create_engine("sqlite:///:memory:", echo=False)
Base.metadata.create_all(engine)

with Profiler() as prof:
    with Session(engine) as session:
        # Inserts
        session.add_all(
            [
                User(name="Alice", email="alice@example.com"),
                User(name="Bob", email="bob@example.com"),
                User(name="Carol", email="carol@example.com"),
            ]
        )
        session.commit()

        # Raw SQL selects
        session.execute(text("SELECT * FROM users"))
        session.execute(text("SELECT * FROM users WHERE id = 1"))
        session.execute(text("SELECT count(*) FROM users"))

        # ORM select
        users = session.query(User).filter(User.name.like("A%")).all()
        print(f"Found: {[u.name for u in users]}")

        # Update
        session.execute(text("UPDATE users SET email = 'new@example.com' WHERE id = 1"))
        session.commit()

        # Delete
        session.execute(text("DELETE FROM users WHERE id = 3"))
        session.commit()

prof.print_stats()
