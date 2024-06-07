import connector
import os

if __name__ == "__main__":
    clear = lambda: os.system('clear')
    clear()
    config_file = 'db_config.json'  # Path to your configuration file
    db = connector.DatabaseConnector()
    db.connect()

    query = "SELECT * FROM users"
    res = db.execute_query(query)
    print(res)