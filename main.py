from utils.file_handler import FileHandler
from utils.database_handler import PostgresHandler
from datetime import datetime

def main_func():
    # adjon egy listát a file-król
    # nyissa meg a file-okat és egy pandas dataframet adjon nekem vissza
    # ezt a dataframet elemezzük ki
    # le kell generálni egy create scriptet
        # típusosítása a mezőnek
    # le kell generálni egy insert scriptet
    # meg kell futtatni a tábla létrehozásokat
    # át kell alakítani az adatot, hogy betölthető legyen
    # betöltjük a feladatot

    file_utils = FileHandler()
    postgres = PostgresHandler()

    file_list = file_utils.get_file_list()

    for item in file_list:
        # dropped = postgres.drop_table(item)
        # if dropped is not True:
        #     print(dropped)

        data = file_utils.get_data_from_csv(item)

        # a file-ból létre kellene hozni egy create table scriptet
        #create_script = generate_table_script(data, item[:-4])
        #postgres.create_object(create_script)
        
        insert_script = generate_insert_script(data.columns, item[:-4])


        data = [tuple(item) for item in data.to_numpy()]

        postgres.insert_many(insert_script, data)

def generate_table_script(csv_data, table_name):
    print('------------------------------------------')
    print(table_name)

    columns = csv_data.columns

    cols = {}
    cols_type = None

    for col in columns:
        col_length = 0
        for item in csv_data[col]:
           
            try:
                if '.' in item:
                    float(item)                    
                    cols_type = 'numeric'
                else:
                    raise
            except:                
                try:
                    int(item)
                    cols_type = 'int'
                except:                    
                    if isinstance(item, bool):
                        cols_type = 'boolean'
                    else:                        
                        if len(item) > col_length:
                            col_length = len(item)
                
                            cols_type = f"varchar({col_length})"

                            try:
                                datetime.strptime(item, '%Y-%m-%d').date()
                                cols_type = 'date'
                            except:
                                pass
                
        cols[col] = cols_type

    create_table_script = f""" create table {table_name} ("""

    for key, value in cols.items():
        create_table_script += f"{key} {value}, "

    create_table_script += f"creation_date date default now())"
            
    return create_table_script


def generate_insert_script(columns, table_name):
    # insert into tabla_neve (mezo1, mezo2) values (ertek1, ertek2);
    insert_statement = f"insert into {table_name} ("

    for idx, item in enumerate(columns):
        insert_statement += f"{item}, " if len(columns) - 1> idx else f"{item}) values ("

    for item in range(0, len(columns)):
        insert_statement += f"%s, " if len(columns) - 1 > item else f"%s)"

    return insert_statement

if __name__ == '__main__':
    main_func()