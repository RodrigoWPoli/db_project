import psycopg2 as psql
import json
import mysql.connector as mysql

class DatabaseConnector:
    def __init__(self, host='localhost', port='3306', database='database', user='root', password='password', db_type='mysql'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.db_type = db_type
        self.connection = None

    def connect(self):
        try:
            if self.db_type == 'postgresql':
                self.connection = psql.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
            elif self.db_type == 'mysql':
                self.connection = mysql.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
            print("Connected to the database!")
        except (psql.Error, mysql.Error) as e:
            print(f"Error connecting to the database: {e}")

    def load_config(self, file_path):
        try:
            with open(file_path, 'r') as file:
                config = json.load(file)
                self.host = config['host']
                self.port = config['port']
                self.database = config['database']
                self.user = config['user']
                self.password = config['password']
                self.db_type = config['db_type']
            print("Config loaded successfully!")
        except FileNotFoundError:
            print(f"Config file not found at {file_path}")
        except json.JSONDecodeError:
            print(f"Error decoding config file at {file_path}")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from the database!")

    def execute_query(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        except (psql.Error, mysql.Error) as e:
            print(f"Error executing query: {e}")
            return None