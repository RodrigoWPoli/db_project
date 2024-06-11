import psycopg2 as psql
import json
import mysql.connector as mysql
from getpass import getpass
from anytree import Node, RenderTree

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
    
    def get_schema(self):
        if self.db_type == 'postgresql':
            query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
            """
            view_query = """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'public';
            """
        elif self.db_type == 'mysql':
            query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
            ORDER BY table_name, ordinal_position;
            """
            view_query = """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = DATABASE();
            """
        else:
            print("Unsupported database type")
            return
        
        schema = self.execute_query(query)
        views = self.execute_query(view_query)
        if schema or views:
            self.print_schema(schema, views)
        else:
            print("No schema information found")

    def print_schema(self, schema, views):
        root = Node("Database Schema")
        tables_node = Node("Tables", parent=root)
        views_node = Node("Views", parent=root)
        
        current_table = None
        table_node = None
        for table_name, column_name, data_type in schema:
            if table_name != current_table:
                table_node = Node(table_name, parent=tables_node)
                current_table = table_name
            Node(f"{column_name} ({data_type})", parent=table_node)

        for view_name in views:
            Node(view_name[0], parent=views_node)

        for pre, fill, node in RenderTree(root):
            print("%s%s" % (pre, node.name))

# Example usage:
# db = DatabaseConnector()
# db.connect()
# db.get_schema()
# db.disconnect()
