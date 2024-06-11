import psycopg2 as psql
import json
import mysql.connector as mysql
from getpass import getpass

class DatabaseConnector:
    def __init__(self, db_config_path='db_config.json'):
        self.host = 'host'
        self.port = 'port'
        self.database = 'database'
        self.user = 'user'
        self.db_type = 'db_type'
        self.connection = None
        self.path = db_config_path
        self.set_config()



    def set_config(self):
        try:
            with open(self.path, 'r') as file:
                self.config_data = json.load(file)
                self.get_config()
        except FileNotFoundError:
            print("Configuration file not found. Creating a new one...")
            with open(self.path, 'w') as file:
                json.dump([], file)
            with open(self.path, 'r') as file:
                self.config_data = json.load(file)
            self.create_config()
        finally:
            self.password = getpass(f"Enter password for {self.user}: ")

    def __del__(self):
        if self.connection:
            self.disconnect()
    
    def reconnect(self):
        self.disconnect()
        self.set_config()
        self.connect()

    def get_config(self):
        config_type = input("Do you want to load an existing database connection? (y/n): ")
        if config_type.lower() == 'y':
            print("Available connections:")
            for i, config in enumerate(self.config_data):
                print(f"Config {i}:")
                print(f"Database: {config['database']}", end=', ')
                print(f"DB Type: {config['db_type']}")
            config_number = int(input("Enter the connection number: "))
            self.load_config(config_number)
        elif config_type.lower() == 'n':
            self.create_config()




    def load_config(self, config_number):
        self.host = self.config_data[config_number]['host']
        self.port = self.config_data[config_number]['port']
        self.database = self.config_data[config_number]['database']
        self.user = self.config_data[config_number]['user']
        self.db_type = self.config_data[config_number]['db_type']
        print("Connection loaded successfully!")
        
    def create_config(self):
        print("Adding a new database connection...")
        self.host = input("Enter the host: ")
        self.port = input("Enter the port: ")
        self.database = input("Enter the database name: ")
        self.user = input("Enter the username: ")
        self.db_type = input("Enter the database type (postgresql/mysql): ")

        config = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'db_type': self.db_type
        }
        self.config_data.append(config)
        with open(self.path, 'w') as file:
            json.dump(self.config_data, file, indent=4)
        


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
            print(f"Now connected to the database {self.database}")
        except (psql.Error, mysql.Error) as e:
            print(f"Error connecting to the database: {e}")

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