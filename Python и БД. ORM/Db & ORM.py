import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, text
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm import declarative_base
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

Base = declarative_base()

class Publisher(Base):
    __tablename__ = 'publisher'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Book(Base):
    __tablename__ = 'book'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    id_publisher = Column(Integer, ForeignKey('publisher.id'))

    publisher = relationship(Publisher, backref='books')


class Shop(Base):
    __tablename__ = 'shop'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Stock(Base):
    __tablename__ = 'stock'
    id = Column(Integer, primary_key=True)
    id_book = Column(Integer, ForeignKey('book.id'))
    id_shop = Column(Integer, ForeignKey('shop.id'))
    count = Column(Integer)

    book = relationship(Book, backref='stocks')
    shop = relationship(Shop, backref='stocks')


class Sale(Base):
    __tablename__ = 'sale'
    id = Column(Integer, primary_key=True)
    price = Column(Float)
    date_sale = Column(Date)
    id_stock = Column(Integer, ForeignKey('stock.id'))
    count = Column(Integer)

    stock = relationship(Stock, backref='sales')


def create_tables(engine):
    Base.metadata.create_all(engine)


def get_sales_by_publisher(session, publisher_input):
    query = (
        session.query(
            Book.title,
            Shop.name,
            Sale.price,
            Sale.date_sale
        )
        .select_from(Publisher)
        .join(Book, Publisher.id == Book.id_publisher)
        .join(Stock, Book.id == Stock.id_book)
        .join(Shop, Shop.id == Stock.id_shop)
        .join(Sale, Stock.id == Sale.id_stock)
    )

    if publisher_input.isdigit():
        results = query.filter(Publisher.id == int(publisher_input)).all()
    else:
        results = query.filter(Publisher.name == publisher_input).all()

    if not results:
        print("Не найдено продаж для данного издателя")
        return

    for title, shop_name, price, date in results:
        print(f"{title} | {shop_name} | {price} | {date.strftime('%d-%m-%Y')}")


def create_database_if_not_exists(db_name, user, password, host='localhost', port=5432):
    try:

        conn = psycopg2.connect(
            dbname='postgres',
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()

        if not exists:
            # Создаем базу данных
            cursor.execute(f'CREATE DATABASE {db_name}')
            print(f"База данных '{db_name}' создана успешно")
        else:
            print(f"База данных '{db_name}' уже существует")

        cursor.close()
        conn.close()

    except psycopg2.OperationalError as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        print("Убедитесь, что:")
        print("1. PostgreSQL запущен")
        print("2. Указаны правильные имя пользователя и пароль")
        print("3. У вас есть права на создание базы данных")
        sys.exit(1)


if __name__ == "__main__":

    DB_NAME = "bookstore"
    DB_USER = "postgres"
    DB_PASSWORD = "password"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    print("Проверка и создание базы данных...")
    create_database_if_not_exists(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

    DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    try:

        engine = create_engine(DSN)
        print("Подключение к базе данных...")

        with engine.connect() as conn:
            print("Успешное подключение к БД!")

        print("Создание таблиц...")
        create_tables(engine)
        print("Таблицы созданы успешно")

        Session = sessionmaker(bind=engine)
        session = Session()

        publisher_input = input("Введите имя или идентификатор издателя: ")

        get_sales_by_publisher(session, publisher_input)

    except sqlalchemy.exc.OperationalError as e:
        print(f"Ошибка подключения к базе данных: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        # Закрываем сессию
        if 'session' in locals():
            session.close()
            print("Сессия закрыта")
