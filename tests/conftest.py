import pytest
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50))


@pytest.fixture
def engine():
    e = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(e)
    with Session(e) as session:
        session.add_all([Item(name="foo"), Item(name="bar"), Item(name="baz")])
        session.commit()
    yield e
    e.dispose()


@pytest.fixture
def session(engine):
    with Session(engine) as s:
        yield s
