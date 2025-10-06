import psycopg2
from psycopg2 import sql
import sys


def create_database(db_name, user, password, host='localhost', port=5432):
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}';")
            exists = cur.fetchone()
            if not exists:
                cur.execute(f"CREATE DATABASE {db_name};")
                print(f"База данных {db_name} создана")
            else:
                print(f"База данных {db_name} уже существует")
        conn.close()
        return True
    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        return False


def create_connection(db_name, user, password, host='localhost', port=5432):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Подключение к базе данных установлено успешно")
        return conn
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return None



def main():
    db_name = "clients_db"
    user = "postgres"
    password = "password"  # ЗАМЕНИТЕ НА ВАШ ПАРОЛЬ

    print("Создание базы данных...")
    if not create_database(db_name, user, password):
        print("Не удалось создать базу данных")
        return

    print("Подключение к базе данных...")
    conn = create_connection(db_name, user, password)
    if conn is None:
        return

    try:

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phones (
                    id SERIAL PRIMARY KEY,
                    client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                    phone VARCHAR(20) UNIQUE
                );
            """)
            conn.commit()
            print("Таблицы созданы успешно")





        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id;",
                ("Иван", "Иванов", "ivan@example.com")
            )
            client1_id = cur.fetchone()[0]
            print(f"Добавлен клиент Иван Иванов с ID: {client1_id}")

            cur.execute(
                "INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id;",
                ("Петр", "Петров", "petr@example.com")
            )
            client2_id = cur.fetchone()[0]
            print(f"Добавлен клиент Петр Петров с ID: {client2_id}")



        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO phones (client_id, phone) VALUES (%s, %s);",
                (client1_id, "+79111111111")
            )
            cur.execute(
                "INSERT INTO phones (client_id, phone) VALUES (%s, %s);",
                (client1_id, "+79112222222")
            )
            cur.execute(
                "INSERT INTO phones (client_id, phone) VALUES (%s, %s);",
                (client2_id, "+79222222222")
            )
            print("Телефоны добавлены")



        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone 
                FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                ORDER BY c.id;
            """)
            for row in cur.fetchall():
                print(row)



        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone 
                FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                WHERE c.last_name = %s;
            """, ("Иванов",))
            for row in cur.fetchall():
                print(row)



        with conn.cursor() as cur:
            cur.execute(
                "UPDATE clients SET email = %s WHERE id = %s;",
                ("ivan.new@example.com", client1_id)
            )
            print("Email обновлен")



        with conn.cursor() as cur:
            cur.execute("DELETE FROM phones WHERE phone = %s;", ("+79112222222",))
            print("Телефон удален")



        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone 
                FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                ORDER BY c.id;
            """)
            for row in cur.fetchall():
                print(row)

        conn.commit()


    except Exception as e:
        print(f"Произошла ошибка: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Соединение с базой данных закрыто")


if __name__ == "__main__":
    main()