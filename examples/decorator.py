"""
Decorator usage - profile specific functions automatically.
Run: uv run examples/decorator.py
"""

import asyncio

from sqlalchemy import Integer, String, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from sqlalchemy_spy import profile


class Base(DeclarativeBase):
    pass


class Article(Base):
    __tablename__ = "articles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(100))
    views: Mapped[int] = mapped_column(Integer, default=0)


engine = create_engine("sqlite:///:memory:", echo=False)
Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add_all(
        [
            Article(title="Getting started with SQLAlchemy", views=1200),
            Article(title="Advanced ORM patterns", views=430),
            Article(title="Profiling Python apps", views=876),
            Article(title="FastAPI best practices", views=2100),
        ]
    )
    session.commit()


@profile(engine)
def get_popular_articles(min_views: int = 500):
    """Get articles above a view threshold."""
    with Session(engine) as session:
        return (
            session.query(Article)
            .filter(Article.views >= min_views)
            .order_by(Article.views.desc())
            .all()
        )


@profile(engine)
def update_views(article_id: int, increment: int = 1):
    """Increment view count for an article."""
    with Session(engine) as session:
        session.execute(
            text("UPDATE articles SET views = views + :inc WHERE id = :id"),
            {"inc": increment, "id": article_id},
        )
        session.commit()


@profile(engine)
async def get_stats():
    """Aggregate stats across all articles."""
    with Session(engine) as session:
        total = session.execute(text("SELECT count(*) FROM articles")).scalar()
        avg = session.execute(text("SELECT avg(views) FROM articles")).scalar()
        top = session.execute(
            text("SELECT title FROM articles ORDER BY views DESC LIMIT 1")
        ).scalar()
        return {"total": total, "avg_views": avg, "top": top}


# Run them
articles = get_popular_articles(500)
print(f"Popular: {[a.title for a in articles]}\n")

update_views(article_id=1, increment=50)


async def main():
    stats = await get_stats()
    print(f"Stats: {stats}")


asyncio.run(main())
