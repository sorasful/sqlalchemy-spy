"""
N+1 query problem - comparison between naive loading and eager loading.
Run: uv run examples/n_plus_one.py
"""

from sqlalchemy import ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    joinedload,
    mapped_column,
    relationship,
)

from sqlalchemy_profiler import Profiler


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50))
    posts: Mapped[list["Post"]] = relationship(
        "Post", back_populates="author", lazy="select"
    )


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(100))
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    author: Mapped["User"] = relationship("User", back_populates="posts")


engine = create_engine("sqlite:///:memory:", echo=False)
Base.metadata.create_all(engine)

with Session(engine) as session:
    users = [User(name=f"User {i}") for i in range(5)]
    session.add_all(users)
    session.flush()
    for u in users:
        session.add_all(
            [
                Post(title=f"Post A by {u.name}", user_id=u.id),
                Post(title=f"Post B by {u.name}", user_id=u.id),
            ]
        )
    session.commit()


print("\n=== N+1 problem (lazy loading) ===")
with Profiler(engine) as prof:
    with Session(engine) as session:
        users = session.query(User).all()  # 1 query
        for u in users:
            _ = u.posts  # 1 query per user  → N+1

prof.print_stats(top_slow=3)


print("\n=== Fixed with joinedload (eager loading) ===")
with Profiler(engine) as prof:
    with Session(engine) as session:
        users = session.query(User).options(joinedload(User.posts)).all()  # 1 query
        for u in users:
            _ = u.posts  # already loaded, no extra query

prof.print_stats(top_slow=3)
