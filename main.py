import connector
import os


def db_connection():
    db = connector.DatabaseConnector()
    db.connect()
    while True:
        clear()
        if (db):
            print(f"Connected to {db.get_database_name()}")
        print("1. Execute query")
        print("2. Show schema")
        print("3. Configurations")
        print("4. Connect to another database")
        print("5. Exit")
        choice = input("Enter your choice: ")
        if choice == '1':
            query = input("Enter your query: ")
            db.execute_query(query)
            input("Press enter to continue...")
        elif choice == '2':
            db.print_schema_tree() 
            input("Press enter to continue...")
        elif choice == '3':
            configurations(db)
        elif choice == '4':
            db.reconnect()
        elif choice == '5':
            clear()
            db.close()
            print("bye!")
            break


def configurations(db):
    db.get_limit()
    print(f"1. Limit query results (Current: {db.get_limit()})")
    choice = input("Enter your choice: ")
    if choice == '1':
        limit = input("Enter the limit: ")
        db.set_limit(limit)
        print("Limit updated successfully!")
        input("Press enter to continue...")


if __name__ == "__main__":
    clear = lambda: os.system('clear')
    clear()
    db_connection()
