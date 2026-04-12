"""
FastAPI example - SQLAlchemyProfilerMiddleware.

The middleware profiles every request and:
- Adds X-DB-Query-Count and X-DB-Total-Time response headers.
- Prints a console report for requests that exceed slow_threshold_ms.

Run:
    uv run --with fastapi --with uvicorn[standard] uvicorn examples.fastapi_app:app --reload

Then try:
    curl http://localhost:8000/users
    curl http://localhost:8000/users/1
    curl http://localhost:8000/stats
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from sqlalchemy import ForeignKey, Integer, String, create_engine, select
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from sqlalchemy_profiler import Profiler


engine = create_engine("sqlite:///example.db", echo=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50))
    posts: Mapped[list["Post"]] = relationship("Post", back_populates="author")


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(100))
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    author: Mapped["User"] = relationship("User", back_populates="posts")


Base.metadata.create_all(engine)

# Seed if empty
with Session(engine) as session:
    if not session.execute(select(User)).first():
        users = [User(name=f"User {i}") for i in range(1, 6)]
        session.add_all(users)
        session.flush()
        session.add_all([Post(title=f"Post by {u.name}", user_id=u.id) for u in users])
        session.commit()


class SQLAlchemyProfilerMiddleware(BaseHTTPMiddleware):
    """Profile SQLAlchemy queries for every request.

    Args:
        slow_threshold_ms: Only print stats when total query time exceeds this
            value. Set to 0 to always print. Default: 0.
        engine: SQLAlchemy engine to scope profiling to. If None, all engines
            are profiled automatically.
    """

    def __init__(self, app, *, slow_threshold_ms: float = 0, engine=None):
        super().__init__(app)
        self.slow_threshold_ms = slow_threshold_ms
        self._engine = engine

    async def dispatch(self, request: Request, call_next) -> Response:
        profiler = Profiler(self._engine)
        profiler.start()

        response = await call_next(request)

        profiler.stop()

        response.headers["X-DB-Query-Count"] = str(profiler.query_count)
        response.headers["X-DB-Total-Time-Ms"] = f"{profiler.total_time_ms:.2f}"

        if (
            profiler.query_count > 0
            and profiler.total_time_ms >= self.slow_threshold_ms
        ):
            print(f"\n[{request.method} {request.url.path}]")
            profiler.print_stats()

        return response


app = FastAPI(title="sqlalchemy-profiler demo")
app.add_middleware(SQLAlchemyProfilerMiddleware, slow_threshold_ms=0)


@app.get("/users")
def list_users():
    with Session(engine) as session:
        return [
            {"id": u.id, "name": u.name}
            for u in session.execute(select(User)).scalars()
        ]


@app.get("/users/{user_id}")
def get_user(user_id: int):
    with Session(engine) as session:
        user = session.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        # Intentional N+1 for demo purposes
        posts = (
            session.execute(select(Post).where(Post.user_id == user_id)).scalars().all()
        )
        return {"id": user.id, "name": user.name, "posts": [p.title for p in posts]}


@app.get("/stats")
def db_stats():
    """Runs several queries so the profiler has something interesting to show."""
    with Session(engine) as session:
        user_count = session.execute(select(User)).scalars().all()
        post_count = session.execute(select(Post)).scalars().all()
        return {"users": len(user_count), "posts": len(post_count)}
