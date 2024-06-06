import mysql.connector
import psycopg2
import json

def connect_to_mysql(config):
    try:
        conn = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        print("Connected to MySQL database")
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def connect_to_postgresql(config):
    try:
        conn = psycopg2.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        print("Connected to PostgreSQL database")
        return conn
    except psycopg2.Error as err:
        print(f"Error: {err}")
        return None

def load_config(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)

if __name__ == "__main__":
    config_file = 'db_config.json'  # Path to your configuration file
    config = load_config(config_file)

    db_type = config['db_type']
    connection = None

    if db_type == 'mysql':
        connection = connect_to_mysql(config)
    elif db_type == 'postgresql':
        connection = connect_to_postgresql(config)
    else:
        print("Unsupported database type")

    if connection:
        # Perform your database operations here
        # For example, create a cursor and execute a query
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"Database version: {version}")

        cursor.close()
        connection.close()
        print("Connection closed")
