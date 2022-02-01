import psycopg2 as db

class PostgresHandler:

    def get_connection(self):
        return db.connect(user="postgres",
                password="postgres",
                host="localhost",
                port=5432,
                database="postgres")