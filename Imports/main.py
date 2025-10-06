from datetime import datetime
from time import sleep
from application.salary import calculate_salary
from application.db.people import get_employees

if __name__ == '__main__':
    print(f'Текущая дата: {datetime.now().date()}')
    sleep(1)

    calculate_salary()
    sleep(1)

    get_employees()
    sleep(1)

    print(f'Дата завершения: {datetime.now().date()}')