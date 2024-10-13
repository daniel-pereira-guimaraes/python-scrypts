# pip install mysql-connector-python

import os
from csv import DictReader
import mysql.connector
from mysql.connector import Error
from mysql.connector.abstracts import MySQLConnectionAbstract

config = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT", 3306)),
    'user': os.getenv("DB_USER", "root"),
    'password': os.getenv("DB_PASSWORD", "root"),
    'charset': os.getenv("DB_CHARSET", 'utf8mb4')
}

database = "store"

def create_database_if_not_exists(connection: MySQLConnectionAbstract):
    print(f"Creating database '{database}' if not exists...")
    with connection.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS " + database)

def select_database(connection: MySQLConnectionAbstract):
    print(f"Selecting database '{database}'...")
    with connection.cursor() as cursor:
        cursor.execute("USE " + database)

def create_country_table_if_not_exists(connection: MySQLConnectionAbstract):
    print("Creating 'country' table if not exists...")
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS country (
                code CHAR(2) NOT NULL,
                name VARCHAR(100) NOT NULL,
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                CONSTRAINT pk_country PRIMARY KEY (code),
                CONSTRAINT un_country_name UNIQUE (name)
            )
        """)

def safe_float(value: str, default: None | float):
    try:
        return float(value)
    except ValueError:
        return default

def insert_country(connection: MySQLConnectionAbstract, data):
    code = data['code']
    name = data['name']
    latitude = safe_float(data['latitude'], None)
    longitude = safe_float(data['longitude'], None)
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT IGNORE INTO country(code, name, latitude, longitude)
            VALUES(%s, %s, %s, %s)
        """, (code, name, latitude, longitude))

def import_countries_from_csv_file(connection: MySQLConnectionAbstract):
    print("Importing countries from CSV file...")
    with open("countries.csv", encoding="utf-8") as file:
        reader = DictReader(file, delimiter=',')
        for row in reader:
            insert_country(connection, row)

def execute_all():
    connection = None
    try:
        connection = mysql.connector.connect(**config)
        create_database_if_not_exists(connection)
        select_database(connection)
        create_country_table_if_not_exists(connection)
        import_countries_from_csv_file(connection)
        connection.commit()
    except Error as e:
        print(f"Error: {e}")
        if connection and connection.is_connected():
            connection.rollback()
    finally:
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    execute_all()
