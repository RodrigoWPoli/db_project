import connector

if __name__ == "__main__":
    config_file = 'db_config.json'  # Path to your configuration file
    db = connector.DatabaseConnector()
    db.load_config(config_file)
    db.connect()

    query = "SELECT * FROM users"
    res = db.execute_query(query)
    print(res)