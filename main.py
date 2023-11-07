import mysql.connector
import pandas as pd
from mysql.connector import Error


def create_connection(host_name, user_name, user_password, database_name=None):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=database_name,
        )
        print("Подключение к базе данных MySQL прошло успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")
    return connection


def create_database(connection):
    try:
        cursor = connection.cursor()
        database_name = "HR_Department_lab6"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"База данных '{database_name}' создана успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")


def create_tables(connection):
    try:
        database_name = "HR_Department_lab6"
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")

        # Создание таблицы "Projects"
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Projects (
        ProjectNumber INT AUTO_INCREMENT PRIMARY KEY,
        ProjectName TEXT,
        Deadline DATE,
        Funding DECIMAL(12, 2) NOT NULL
        );

        ''')

        # Создание таблицы "Positions"
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Positions (
            PositionID INT AUTO_INCREMENT PRIMARY KEY,
            PositionName TEXT,
            Salary DECIMAL(10, 2),
            BonusPercentage DECIMAL(5, 2) CHECK (BonusPercentage BETWEEN 0.00 AND 100.00)
        );
        ''')

        # Создание таблицы "Departments"
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Departments (
                DepartmentID INT AUTO_INCREMENT PRIMARY KEY,
                DepartmentName TEXT,
                PhoneNumber TEXT,
                RoomNumber INT CHECK (RoomNumber BETWEEN 701 AND 710),
                UNIQUE (RoomNumber)
            )
        ''')
        # Создание таблицы "Employees"
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Employees (
                EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
                LastName TEXT,
                FirstName TEXT,
                MiddleName TEXT,
                Address TEXT,
                Phone TEXT,
                Education ENUM('спеціальна', 'середня', 'вища'),
                DepartmentID INT,
                PositionID INT,
                FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID),
                FOREIGN KEY (PositionID) REFERENCES Positions(PositionID),
                CHECK (Education IN ('спеціальна', 'середня', 'вища'))
            )
        ''')


        # Создание таблицы "ProjectExecution"
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ProjectExecution (
        ExecutionID INT AUTO_INCREMENT PRIMARY KEY,
        ProjectNumber INT,
        DepartmentID INT,
        StartDate DATE,
        FOREIGN KEY (ProjectNumber) REFERENCES Projects(ProjectNumber),
        FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
        );

        ''')

        connection.commit()
        print("Таблицы созданы успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Запрос выполнен успешно.")
    except Error as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")


def execute_query_print(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразовать результат в объект DataFrame
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])

        # Вывести DataFrame
        print(df)

        print("Запит виконано успішно.")
    except Error as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")
    finally:
        cursor.close()


def display(connection):
    connect = connection

    print("Відобразити всіх робітників, які мають оклад більший за 2000 грн. Відсортувати прізвища за алфавітом:")
    query = ("""
    SELECT E.LastName, E.FirstName, E.MiddleName, P.Salary
    FROM Employees AS E
    JOIN Positions AS P ON E.PositionID = P.PositionID
    WHERE P.Salary > 2000
    ORDER BY E.LastName;
    """)
    execute_query_print(connect, query)

    print("Порахувати середню зарплатню в кожному відділі (підсумковий запит):")
    query = ("""
        SELECT D.DepartmentName, AVG(P.Salary) AS AverageSalary
        FROM Employees AS E
        JOIN Departments AS D ON E.DepartmentID = D.DepartmentID
        JOIN Positions AS P ON E.PositionID = P.PositionID
        GROUP BY D.DepartmentName;
        """)

    execute_query_print(connect, query)

    print("Відобразити всі проекти, які виконуються в обраному відділі (запит з параметром):")
    id_dep = input()
    query = (f"""
    SELECT P.ProjectName, P.Deadline, P.Funding
    FROM Projects AS P
    JOIN ProjectExecution AS PE ON P.ProjectNumber = PE.ProjectNumber
    WHERE PE.DepartmentID = {id_dep};
    """)

    execute_query_print(connect, query)

    print("Порахувати кількість працівників у кожному відділу (підсумковий запит):")
    query = (f"""
        SELECT D.DepartmentName, COUNT(*) AS EmployeeCount
        FROM Employees AS E
        JOIN Departments AS D ON E.DepartmentID = D.DepartmentID
        GROUP BY D.DepartmentName;
        """)

    execute_query_print(connect, query)

    print("Порахувати розмір премії для кожного співробітника (запит з обчислювальним полем):")

    query = ("""
        SELECT E.LastName, E.FirstName, E.MiddleName, (P.Salary * P.BonusPercentage / 100) AS BonusAmount
        FROM Employees AS E
        JOIN Positions AS P ON E.PositionID = P.PositionID;
        """)
    execute_query_print(connect, query)

    print("Порахувати кількість робітників, які мають спеціальну, середню, вищу освіту у кожному відділу (перехресний запит):")

    query = ("""
        SELECT DepartmentID, 
       SUM(CASE WHEN Education = 'Спеціальна' THEN 1 ELSE 0 END) AS SpecialEducationCount,
       SUM(CASE WHEN Education = 'Середня' THEN 1 ELSE 0 END) AS SecondaryEducationCount,
       SUM(CASE WHEN Education = 'Вища' THEN 1 ELSE 0 END) AS HigherEducationCount
        FROM Employees
        GROUP BY DepartmentID;
        """)
    execute_query_print(connect, query)


def insert_tables(conn_insert):
    conn_insert = conn_insert
    #Positions
    query = (f"""
            INSERT INTO Positions (PositionName, Salary, BonusPercentage)
        VALUES
    ('Інженер', 50000.00, 5.0),
    ('Редактор', 45000.00, 4.0),
    ('Програміст', 55000.00, 6.0);
                """)
    execute_query(conn_insert, query)
    #Departments
    query = ("""
        INSERT INTO Departments (DepartmentName, PhoneNumber, RoomNumber)
    VALUES
    ('Програмування', '(123) 456-7890', 701),
    ('Дизайн', '(234) 567-8901', 702),
    ('Інформаційні технології', '(345) 678-9012', 703);     
        """)
    execute_query(conn_insert, query)

    # Додавання даних в таблицю "Співробітники" для 17 працівників
    query = ('''
    INSERT INTO Employees (LastName, FirstName, MiddleName, Address, Phone, Education, DepartmentID, PositionID)
    VALUES
        ('Прізвище1', 'Імя1', 'По батькові1', 'Адреса1', '(123) 456-7890', 'спеціальна', 1, 1),
        ('Прізвище2', 'Імя2', 'По батькові2', 'Адреса2', '(234) 567-8901', 'вища', 2, 2),
        ('Прізвище3', 'Імя3', 'По батькові3', 'Адреса3', '(345) 678-9012', 'середня', 3, 3),
        ('Прізвище4', 'Імя4', 'По батькові4', 'Адреса4', '(456) 789-0123', 'спеціальна', 1, 1),
        ('Прізвище5', 'Імя5', 'По батькові5', 'Адреса5', '(567) 890-1234', 'вища', 2, 2),
        ('Прізвище6', 'Імя6', 'По батькові6', 'Адреса6', '(678) 901-2345', 'середня', 3, 3),
        ('Прізвище7', 'Імя7', 'По батькові7', 'Адреса7', '(789) 012-3456', 'спеціальна', 1, 1),
        ('Прізвище8', 'Імя8', 'По батькові8', 'Адреса8', '(890) 123-4567', 'вища', 2, 2),
        ('Прізвище9', 'Імя9', 'По батькові9', 'Адреса9', '(901) 234-5678', 'середня', 3, 3),
        ('Прізвище10', 'Імя10', 'По батькові10', 'Адреса10', '(012) 345-6789', 'спеціальна', 1, 1),
        ('Прізвище11', 'Імя11', 'По батькові11', 'Адреса11', '(123) 456-7890', 'вища', 2, 2),
        ('Прізвище12', 'Імя12', 'По батькові12', 'Адреса12', '(234) 567-8901', 'середня', 3, 3),
        ('Прізвище13', 'Імя13', 'По батькові13', 'Адреса13', '(345) 678-9012', 'спеціальна', 1, 1),
        ('Прізвище14', 'Імя14', 'По батькові14', 'Адреса14', '(456) 789-0123', 'вища', 2, 2),
        ('Прізвище15', 'Імя15', 'По батькові15', 'Адреса15', '(567) 890-1234', 'середня', 3, 3),
        ('Прізвище16', 'Імя16', 'По батькові16', 'Адреса16', '(678) 901-2345', 'спеціальна', 1, 1),
        ('Прізвище17', 'Імя17', 'По батькові17', 'Адреса17', '(789) 012-3456', 'вища', 2, 2);
        ''')

    execute_query(conn_insert, query)

    query = ('''
    INSERT INTO Projects (ProjectName, Deadline, Funding)
    VALUES
        ('Проект1', '2023-12-31', 10000.00),
        ('Проект2', '2023-11-30', 15000.00),
        ('Проект3', '2023-10-30', 12000.00),
        ('Проект4', '2023-09-30', 18000.00),
        ('Проект5', '2023-08-30', 9000.00),
        ('Проект6', '2023-07-30', 20000.00),
        ('Проект7', '2023-06-30', 17000.00),
        ('Проект8', '2023-05-30', 13000.00);
''')
    execute_query(conn_insert, query)

    query = ('''
    INSERT INTO ProjectExecution (ProjectNumber, DepartmentID, StartDate)
    VALUES
        (1, 1, '2023-11-05'),
        (2, 2, '2023-11-06'),
        (3, 3, '2023-11-07'),
        (2, 1, '2023-11-08'),
        (1, 2, '2023-11-09');
    ''')
    execute_query(conn_insert, query)
    conn_insert.close()


if __name__ == "__main__":
    config = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
    }
    config1 = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
        'database_name': 'HR_Department_lab6',
    }
    # Подключение к серверу MySQL
    conn = create_connection(**config)

    # Создание базы данных
    create_database(conn)

    # Создание таблиц
    create_tables(conn)
    conn.close()
    conn = create_connection(**config1)
    # Insert Tables

    insert_tables(conn)

    # Display
    conn = create_connection(**config1)
    display(conn)

    conn.close()

    print("База данных и таблицы созданы успешно.")
