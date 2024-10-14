import json
import os
import mysql.connector
from mysql.connector import Error
from decimal import Decimal

config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 3306),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD','root'),
    'charset': os.getenv("DB_CHARSET", 'utf8mb4'),
    'database': 'store'
}

json_file_path = "countries.json"

def export_countries(connection):
    print(f"Exporting to {json_file_path}...")
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM country')
        export_cursor(cursor)

def export_cursor(cursor):
    with open(json_file_path, 'w') as file:
        file.write('[\n')
        export_rows(cursor, file)
        file.write('\n]')

def export_rows(cursor, file):
    add_comma = False
    while row := cursor.fetchone():
        if add_comma:
            file.write(',\n')
        file.write(json.dumps(row, default=default_conversion))
        add_comma = True

def default_conversion(value):
    if isinstance(value, Decimal):
        return float(value)
    return str(value)

def execute_all():
    print("Connecting to database...")
    with mysql.connector.connect(**config) as connection:
        try:
            export_countries(connection)
        except Error as e:
            print(f'Database error: {e}')
        else:
            print("Done!")

if __name__ == "__main__":
    execute_all()