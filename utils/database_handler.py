import psycopg2 as db
if __name__ == '__main__':
    from params import (POSTGRES_DB, POSTGRES_HOST,
                POSTGRES_PASSWORD, 
                POSTGRES_PORT, 
                POSTGRES_USER)
else:
    from utils.params import (POSTGRES_DB, POSTGRES_HOST,
                    POSTGRES_PASSWORD, 
                    POSTGRES_PORT, 
                    POSTGRES_USER)

class PostgresHandler:

    def get_connection(self):
        return db.connect(user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB)

    def drop_table(self, table_name):
        with self.get_connection() as con:
            with con.cursor() as cur:
                try:
                    # productionben, ne
                    cur.execute(f"drop table {table_name}")
                except (Exception, db.DatabaseError) as error:
                    return False, error

        return True

    def insert_many(self, insert_statement, data):
        with self.get_connection() as con:
            with con.cursor() as cur:
                try:
                    cur.executemany(insert_statement, data) # az adatnak [(),(),(),()]
                except (Exception, db.DatabaseError) as error:
                    return False, error

        return True

    def create_object(self, object_script):
        with self.get_connection() as con:
            with con.cursor() as cur:
                try:
                    cur.execute(object_script)
                except (Exception, db.DatabaseError) as error:
                    return False, error

        return True