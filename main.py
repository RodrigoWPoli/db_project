from time import sleep
import connector
import os


def db_connection():
    db = connector.DatabaseConnector()
    db.connect()
    sleep(3)
    while True:
        clear()
        print("1. Execute query")
        print("2. Connect to another database")
        print("3. Exit")
        choice = input("Enter your choice: ")
        if choice == '1':
            query = input("Enter your query: ")
            res = db.execute_query(query)
            print(res)
            input("Press enter to continue...")
        elif choice == '2':
            db.reconnect()
        elif choice == '3':
            break

if __name__ == "__main__":
    clear = lambda: os.system('clear')
    clear()
    db_connection()
