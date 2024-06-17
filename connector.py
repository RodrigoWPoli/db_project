import psycopg2 as psql
import mysql.connector as mysql
from getpass import getpass
from prettytable import PrettyTable
from anytree import Node, RenderTree
import csv
import json
from datetime import datetime

class DatabaseConnector:
    def __init__(self, db_config_path='db_config.json'):
        self.__host = 'host'
        self.__port = 'port'
        self.__database = 'database'
        self.__user = 'user'
        self.__db_type = 'db_type'
        self.__connection = None
        self.__limit = 1000
        self.__path = db_config_path
        self.config_index = None
        self.set_config()

    def __del__(self):
        self.close()

    def close(self):
        if self.__connection:
            self.disconnect()
            self.__connection = None

    def get_database_name(self):
        return self.__database
    
    def get_limit(self):
        return self.__limit
    
    def set_limit(self, limit):
        self.__limit = limit

    def set_config(self):
        try:
            with open(self.__path, 'r') as file:
                self.config_data = json.load(file)
                if not self.config_data:
                    raise FileNotFoundError
                self.get_config()
        except FileNotFoundError:
            print("Configuration file not found. Creating a new one...")
            with open(self.__path, 'w') as file:
                json.dump([], file)
            with open(self.__path, 'r') as file:
                self.config_data = json.load(file)
            self.create_config()

    def get_config(self):
        config_type = input("Do you want to load an existing database connection? (y/n): ")
        if config_type.lower() == 'y':
            print("Available connections:")
            for i, config in enumerate(self.config_data):
                print(f"Config {i}:")
                print(f"Database: {config['database']}", end=', ')
                print(f"DB Type: {config['db_type']}")
            config_number = int(input("Enter the connection number: "))
            self.config_index = config_number
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
        self.config_index = len(self.config_data) - 1
        with open(self.__path, 'w') as file:
            json.dump(self.config_data, file, indent=4)

    def connect(self):
        self.password = getpass(f"Enter password for {self.__user}: ")
        if self.__db_type not in ['postgresql', 'mysql']:
            print("Invalid database type. Please use 'postgresql' or 'mysql'.")
            self.get_config()
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
            if "does not exist" in str(e):
                print(f"Database {self.__database} does not exist.")
                fix = input("Do you want to fix the database connection information? (y/n): ")
                if fix.lower() == 'y':
                    self.modify_config()
                else:
                    self.get_config()
            else:
                print(f"Error connecting to the database: {e}")
                self.connect()

    def modify_config(self):
        if self.config_index is None:
            print("No configuration loaded to modify.")
            return

        print("Modifying the current database connection...")
        print("Current configuration:")
        print(f"Host: {self.__host}")
        print(f"Port: {self.__port}")
        print(f"Database: {self.__database}")
        print(f"User: {self.__user}")
        print(f"DB Type: {self.__db_type}")

        self.__host = input("Enter the host (leave empty to keep the current value): ") or self.__host
        self.__port = input("Enter the port (leave empty to keep the current value): ") or self.__port
        self.__database = input("Enter the database name (leave empty to keep the current value): ") or self.__database
        self.__user = input("Enter the username (leave empty to keep the current value): ") or self.__user
        self.__db_type = input("Enter the database type (postgresql/mysql) (leave empty to keep the current value): ") or self.__db_type

        config = {
            'host': self.__host,
            'port': self.__port,
            'database': self.__database,
            'user': self.__user,
            'db_type': self.__db_type
        }
        self.config_data[self.config_index] = config
        with open(self.__path, 'w') as file:
            json.dump(self.config_data, file, indent=4)
        self.connect()

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
            # will check if query has a limit clause and is a select, if it doesn't have, it will add one
            if "LIMIT" not in query.upper() and query.upper().startswith("SELECT"):
                query = f"{query.strip().rstrip(';')} LIMIT {self.__limit};"

            cursor.execute(query)
            # will check if the query is a select, if it is, it will fetch the results and print them
            if query.upper().startswith("SELECT"):
                results = cursor.fetchall()
                description = cursor.description
                self.print_query_results(results, description)

                save = input(f"Do you want to save the results to a CSV file? (y/n)")
                if save.lower() == 'y':
                    self.save_query(results, description)
            # if the query is not a select, it will commit the changes
            else:
                self.__connection.commit()
                print("Query executed successfully.")
        except (psql.Error, mysql.Error) as e:
            print(f"Error executing query: {e}")
        finally:
            if cursor:
                cursor.close()


    def save_query(self, results, description):
        if not results or not description:
            print("No results to save.")
            return

        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"queries/{self.__database}_{now}.csv"

        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([desc[0] for desc in description]) 
            writer.writerows(results)

        print(f"Query results saved to {filename}")

    def print_schema_tree(self):
        try:
            root_node = Node(self.__database)
            tables_node = Node('Tables', parent = root_node)
            views_node = Node('Views', parent = root_node)

            if self.__db_type == 'postgresql':
                table_query = """
                WITH primary_keys AS (
                    SELECT
                        kcu.table_schema,
                        kcu.table_name,
                        kcu.column_name
                    FROM
                        information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        AND tc.table_name = kcu.table_name
                    WHERE
                        tc.constraint_type = 'PRIMARY KEY'
                )
                SELECT
                    cols.table_name,
                    cols.column_name,
                    cols.data_type,
                    COALESCE(cols.character_maximum_length::int, cols.numeric_precision) AS length,
                    CASE
                        WHEN pk.column_name IS NOT NULL THEN 'YES'
                        ELSE 'NO'
                    END AS is_primary_key
                FROM
                    information_schema.columns cols
                LEFT JOIN primary_keys pk
                    ON cols.table_schema = pk.table_schema
                    AND cols.table_name = pk.table_name
                    AND cols.column_name = pk.column_name
                WHERE
                    cols.table_schema = 'public'
                ORDER BY
                    cols.table_schema,
                    cols.table_name,
                    cols.ordinal_position;
                            """
                view_query = """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = 'public';
                """
            elif self.__db_type == 'mysql':
                table_query = """
                SELECT table_name, column_name, column_type, column_key
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                ORDER BY table_name, ordinal_position;
                """
                view_query = """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = DATABASE();
                """

            #get schema's tables
            cursor = self.__connection.cursor()
            cursor.execute(table_query)
            attributes = cursor.fetchall()

            table_node = None
            current_table = None
            if self.__db_type == "mysql":
                for table, att_name, type, key in attributes:
                    if table != current_table:
                        table_node = Node(table, parent = tables_node)
                        current_table = table

                    att_string = f"{att_name}  {type}  {key}"
                    Node(att_string, parent = table_node)
            else:
                for table, att, att_type, length, is_pk in attributes:
                    if table != current_table:
                        table_node = Node(table, parent= tables_node)
                        current_table = table

                    att_string = f"{att}  {att_type}"
                    if length != None:
                        att_string = att_string + f"({length})"
                    if is_pk == 'YES':
                        att_string = att_string + '  PK'
                    Node(att_string, parent = table_node)

            #get views
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
        except Exception as e:
            print(f"Error getting schema: {e}")
