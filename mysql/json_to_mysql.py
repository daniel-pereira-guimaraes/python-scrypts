# pip install mysql-connector-python

import os
import json
import mysql.connector
from mysql.connector import Error

config = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT", 3306)),
    'database': os.getenv("DB_DATABASE", "store"),
    'user': os.getenv("DB_USER", "root"),
    'password': os.getenv("DB_PASSWORD", "root"),
    'charset': os.getenv("DB_CHARSET", 'utf8mb4')
}

sql_insert = """
    INSERT INTO country(code, name, latitude, longitude)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        latitude = VALUES(latitude),
        longitude = VALUES(longitude)
    """

def execute_all():
    print("Connecting to database...")
    with mysql.connector.connect(**config) as connection:
        try:
            import_json_file(connection)
        except Error as e:
            print(f'Database error: {e}')
        else:
            print("Done!")

def import_json_file(connection):
    with open('countries.json', 'r') as file:
        countries = json.load(file)
        insert_countries(connection, countries)

def insert_countries(connection, countries):
    with connection.cursor() as cursor:
        for country in countries:
            values = build_values(country)
            print(f"Importing...{values}")
            cursor.execute(sql_insert, values)

def build_values(country):
    code = country['code']
    name = country['name']
    latitude = country['latitude']
    longitude = country['longitude']
    return code, name, latitude, longitude

if __name__ == "__main__":
    execute_all()