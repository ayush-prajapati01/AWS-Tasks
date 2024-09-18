'''

    @Author: Ayush Prajapati
    @Date: 13-09-2024 
    @Last Modified by: Ayush Prajapati
    @Last Modified time: 13-09-2024 
    @Title: Python program to perform crud operations on 
            SQL Server on AWS RDS

'''


import pyodbc
import os
from dotenv import load_dotenv


def create_database(cursor, db_name):
    # Create a new database
    create_db_query = f"CREATE DATABASE {db_name};"
    cursor.execute(create_db_query)
    cursor.commit()
    print(f"Database '{db_name}' created successfully!")


def create_table(cursor, db_name):
    # Switch to the newly created database
    cursor.execute(f"USE {db_name}")
    cursor.commit()

    # Create a new table
    create_table_query = """
    CREATE TABLE employees (
        id INT PRIMARY KEY IDENTITY(1,1),
        name NVARCHAR(50),
        position NVARCHAR(50),
        salary DECIMAL(18, 2)
    )
    """
    cursor.execute(create_table_query)
    cursor.commit()
    print("Table 'employees' created successfully!")


def insert_data(cursor):
    # Insert data into the employees table
    insert_data_query = """
    INSERT INTO employees (name, position, salary)
    VALUES (?, ?, ?)
    """
    
    # List of employee data
    employee_data = [
        ('Alice Smith', 'Data Analyst', 65000.00),
        ('Bob Johnson', 'System Administrator', 72000.00),
        ('Charlie Brown', 'DevOps Engineer', 85000.00),
        ('Diana Prince', 'Database Administrator', 78000.00),
        ('Edward Norton', 'Software Developer', 90000.00),
        ('Frank Castle', 'Security Engineer', 82000.00),
        ('Grace Hopper', 'Machine Learning Engineer', 95000.00),
        ('Henry Ford', 'Backend Developer', 88000.00),
        ('Irene Adler', 'Frontend Developer', 68000.00),
        ('Jack Reacher', 'Cloud Engineer', 87000.00),
        ('Karen Page', 'IT Support Specialist', 57000.00),
        ('Luke Cage', 'Network Engineer', 74000.00)
    ]

    # Execute the insert query for each employee
    for employee in employee_data:
        cursor.execute(insert_data_query, employee)
    cursor.commit()
    
    print("Data inserted into 'employees' table successfully!")


def read_data(cursor):
    select_query = "SELECT * FROM employees"

    cursor.execute(select_query)
    rows = cursor.fetchall()

    if rows:
        print("--------------------------------------------------------")
        for row in rows:
            print(f"{row.id} | {row.name} | {row.position} | {row.salary}")
    else:
        print("No data found in the 'employees' table.")


def update_employee_salary(cursor, name, new_salary):
    # Update salary for the employee
    update_query = "UPDATE employees SET salary = ? WHERE name = ?"
    cursor.execute(update_query, (new_salary, name))
    cursor.commit()
    
    print(f"Salary updated for employee '{name}'.")


def delete_employee(cursor, name):
    # Delete the employee record
    delete_query = "DELETE FROM employees WHERE name = ?"
    cursor.execute(delete_query, (name,))
    cursor.commit()
    
    print(f"Employee '{name}' deleted successfully!")


def main():

    load_dotenv()

    server = os.getenv('db_server')
    USERNAME = os.getenv('db_username')
    PASSWORD = os.getenv('db_password')

    
    try:
        connectionString = (
            f"""DRIVER={{ODBC Driver 18 for SQL Server}};
            SERVER={server};
            UID={USERNAME};
            PWD={PASSWORD};
            TrustServerCertificate=yes;"""
        )
        connection = pyodbc.connect(connectionString, autocommit=True)
        cursor = connection.cursor()
        print("Connected to the database successfully!")

    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")

    
    while True:
        print("\nSelect an operation:")
        print("1. Create Database")
        print("2. Create Table")
        print("3. Insert Data")
        print("4. Read Table")
        print("5. Update Employee Salary")
        print("6. Delete Employee")
        print("7. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            db_name = input("Enter the database name: ")
            create_database(cursor, db_name)

        elif choice == '2':
            db_name = input("Enter the database name: ")
            create_table(cursor, db_name)

        elif choice == '3':
            insert_data(cursor)

        elif choice == '4':
            read_data(cursor)

        elif choice == '5':
            print("The data is: \n")
            read_data(cursor)
            employee_name = input("\nEnter the employee's name to update: ")
            new_salary = float(input("Enter the new salary: "))
            update_employee_salary(cursor, employee_name, new_salary)

        elif choice == '6':
            employee_name = input("Enter the employee's name to delete: ")
            delete_employee(cursor, employee_name)

        elif choice == '7':
            print("Exiting program.")
            break

        else:
            print("Invalid choice. Please select a valid option.")


if __name__ == "__main__":
    main()







