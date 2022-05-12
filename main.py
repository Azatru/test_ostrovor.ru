'''Задача 1: Что может быть проще SQL?
Вам дана таблица в postgres, которая представляет из себя список сотрудников с их зарплатами и отделами.
Необходимо написать запрос, который будет выбирать человека с максимальной зарплатой из каждого отдела.'''


import psycopg2
from psycopg2 import Error

try:
    connection = psycopg2.connect(
        user='postgres',
        password='1111',
        host='127.0.0.1',
        port='5432',
        database='employee'
    )  # объект подключения к экземпляру базы данных PostgreSQL

    cursor = connection.cursor() # взаимодействие с БД через класс cursor

    print('Информация о сервере PostreSQL')
    print(connection.get_dsn_parameters(), '\n')  # свойство соединения с БД

    cursor.execute('''SELECT m.name, m.department, t.mx 
    FROM (SELECT department, max(salary) as mx from employee 
    GROUP BY department) t 
    JOIN employee m on m.department = t.department and t.mx = m.salary;''')  # Запрос к базе PostgreSQL

    record = cursor.fetchall()  # просмотр результата запроса
    print('Сотрудники с максимальной зарплатой в каждом отделе', record, '\n')

except (Exception, Error) as error:
    print('Ошибка при работе с PostreSQL', error)
finally:
    if connection:
        cursor.close()  # закрытие объекта cursor
        connection.close()  # закрытие объекта connection
        print('Соединение с PostreSQL закрыто')


