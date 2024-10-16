# pip install mysql-connector-python

import os
import csv
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

def write_column_headers(cursor, writer):
    column_names = [desc[0] for desc in cursor.description]
    writer.writerow(column_names)

def export_to_writer(connection, query, writer):
    with connection.cursor() as cursor:
        cursor.execute(query)
        write_column_headers(cursor, writer)
        while row := cursor.fetchone():
            writer.writerow(row)

def export_countries_to_csv_file(connection):
    print("Exporting countries to CSV file...")
    with open("country.csv", mode="w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file, delimiter=';')
        query = "SELECT * FROM country"
        export_to_writer(connection, query, writer)

def execute_all():
    connection = None
    try:
        print("Connecting to database...")
        connection = mysql.connector.connect(**config)
        export_countries_to_csv_file(connection)
    except Error as e:
        print(f"Error: {e}")
    else:
        print("Done!")
    finally:
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    execute_all()