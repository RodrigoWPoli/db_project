import psycopg2 as psql
import json
import mysql.connector as mysql
from getpass import getpass
from prettytable import PrettyTable
from anytree import Node, RenderTree
import csv
from datetime import datetime

class DatabaseConnector:
    def __init__(self, db_config_path='db_config.json'):
        self.__host = 'host'
        self.__port = 'port'
        self.__database = 'database'
        self.__user = 'user'
        self.__db_type = 'db_type'
        self.__connection = None
        self.__path = db_config_path
        self.set_config()

    def __del__(self):
        if self.__connection:
            self.disconnect()
            self.__connection = None
    
    def get_database_name(self):
        return self.__database

    def set_config(self):
        try:
            with open(self.__path, 'r') as file:
                self.config_data = json.load(file)
                self.get_config()
        except FileNotFoundError:
            print("Configuration file not found. Creating a new one...")
            with open(self.__path, 'w') as file:
                json.dump([], file)
            with open(self.__path, 'r') as file:
                self.config_data = json.load(file)
            self.create_config()
        finally:
            self.password = getpass(f"Enter password for {self.__user}: ")

    def get_config(self):
        config_type = input("Do you want to load an existing database connection? (y/n): ")
        if config_type.lower() == 'y':
            print("Available connections:")
            for i, config in enumerate(self.config_data):
                print(f"Config {i}:")
                print(f"Database: {config['database']}", end=', ')
                print(f"DB Type: {config['db_type']}")
            config_number = int(input("Enter the connection number: "))
            self.__host = self.config_data[config_number]['host']
            self.__port = self.config_data[config_number]['port']
            self.__database = self.config_data[config_number]['database']
            self.__user = self.config_data[config_number]['user']
            self.__db_type = self.config_data[config_number]['db_type']
            print("Connection loaded successfully!")
        elif config_type.lower() == 'n':
            self.create_config()
        
    def create_config(self):
        print("Adding a new database connection...")
        self.__host = input("Enter the host: ")
        self.__port = input("Enter the port: ")
        self.__database = input("Enter the database name: ")
        self.__user = input("Enter the username: ")
        self.__db_type = input("Enter the database type (postgresql/mysql): ")

        config = {
            'host': self.__host,
            'port': self.__port,
            'database': self.__database,
            'user': self.__user,
            'db_type': self.__db_type
        }
        self.config_data.append(config)
        with open(self.__path, 'w') as file:
            json.dump(self.config_data, file, indent=4)
        
    def connect(self):
        try:
            if self.__db_type == 'postgresql':
                self.__connection = psql.connect(
                    host=self.__host,
                    port=self.__port,
                    database=self.__database,
                    user=self.__user,
                    password=self.password
                )
            elif self.__db_type == 'mysql':
                self.__connection = mysql.connect(
                    host=self.__host,
                    port=self.__port,
                    database=self.__database,
                    user=self.__user,
                    password=self.password
                )
        except (psql.Error, mysql.Error) as e:
            print(f"Error connecting to the database: {e}")
            
    def disconnect(self):
        if self.__connection:
            self.__connection.close()
            print("Disconnected from the database!")

    def reconnect(self):
        self.disconnect()
        self.set_config()
        self.connect()

    def print_query_results(self, query_results, column_description, max_width=20):
        if not query_results:
            print("No results to display.")
            return
        
        table = PrettyTable()
        if column_description:
            table.field_names = [desc[0] for desc in column_description]
        
        for row in query_results:
            table.add_row(row)
        
        for field in table.field_names:
            table.max_width[field] = max_width
        
        print(table)
    
    def execute_query(self, query):
        try:
            cursor = self.__connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            description = cursor.description
            cursor.close()
            self.print_query_results(results, description)
            save = input(f"Do you want to save the results to a CSV file? (y/n)")
            if save.lower() == 'y' :
                self.save_query(results, description)
        except (psql.Error, mysql.Error) as e:
            raise f"Error executing query: {e}"
    
    def save_query(self, results, description):
        if not results or not description:
            print("No results to save.")
            return
        
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = description[0][0] if description else 'query'
        filename = f"queries/{self.__database}_{now}.csv"

        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([desc[0] for desc in description]) 
            writer.writerows(results)
        
        print(f"Query results saved to {filename}")
    
    def get_schema(self):
        if self.__db_type == 'postgresql':
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
        elif self.__db_type == 'mysql':
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

    
    def print_schema_tree(self):
        root_node = Node(self.__database)
        tables_node = Node('Tables', parent = root_node)
        views_node = Node('Views', parent = root_node)

        #get schema's tables
        cursor = self.__connection.cursor()
        table_query = f"show tables from {self.__database};"
        cursor.execute(table_query)
        tables = cursor.fetchall()
        
        #For each schema, get its columns
        for table in tables:
            table_node = Node(table[0], parent = tables_node)
            cursor.execute(f"show fields from {table[0]};")
            fields = cursor.fetchall()
            for field in fields:
                field_string = field[0] + '  ' + field[1] + '  ' + field[3]
                Node(field_string, parent = table_node)

        #get views
        view_query = """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = DATABASE();
            """
        cursor.execute(view_query)
        views = cursor.fetchall()
        #Add a node under views for each view
        for view in views:
            Node(view[0], parent = views_node)
        
        print('\n')
        for pre, _fill, node in RenderTree(root_node):
            print("%s%s" % (pre, node.name))
        
        print('\n')
        #Closes the connection
        cursor.close()
