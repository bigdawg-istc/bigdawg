import psycopg2
import os


class CatalogClient(object):

    def __init__(self, catalog_cred, database=None):
        """
        Establishes connection to Catalog database

        :param database: database name. If None, connects to default.
        :param user: username
        :param password: password
        :param host: hostname for database
        :param port: port
        """
        self.user = catalog_cred["user"]
        self.password = catalog_cred["password"]
        self.host = catalog_cred["host"]
        self.port = catalog_cred["port"]
        self.conn = psycopg2.connect(database=database, user=self.user, password=self.password, host=self.host, port=self.port)
        print "Opened database successfully"

    def __del__(self):
        """
        Destructor to close the connection
        """
        self.conn.close()
        print "Connection closed"

    def exec_sql_script(self, fn):
        """
        Reads an SQL script from a file and executes all lines.
        """
        # Read sql file
        assert(os.path.isfile(fn))
        with open(fn, 'r') as f:
            sql = f.read()

        # Execute
        cur = self.conn.cursor()
        cur.execute(sql)
        cur.close()
        self.conn.commit()
        print "Inserted data"

    def insert_database_info(self, catalog_db_params):
        """
        Connects to the catalog.databases table and inserts one row of database metadata
        """
        cur = self.conn.cursor()
        cur.execute("INSERT INTO catalog.databases values(%s, %s, %s, %s, %s);", (catalog_db_params['dbid'], catalog_db_params['engine_id'], catalog_db_params['name'], catalog_db_params['userid'], catalog_db_params['password']))
        cur.close()
        self.conn.commit()
        print "Inserted database info into catalog"

    def insert_object_info(self, catalog_object_params):
        """
        Connects to the catalog.objects table and inserts one row of object metadata
        """
        cur = self.conn.cursor()
        cur.execute("INSERT INTO catalog.objects values(%s, %s, %s, %s, %s);", (catalog_object_params['oid'], catalog_object_params['name'], catalog_object_params['fields'], catalog_object_params['logical_db'], catalog_object_params['physical_db']))
        cur.close()
        self.conn.commit()
        print "Inserted object info into catalog"

if __name__ == "__main__":

    # Credientials for connecting to database
    catalog_cred = {"user": "pguser",
                    "password": "test",
                    "host": "saw.csail.mit.edu",
                    "port": "5400"}

    # Parameters for creating new database/table and inserting data
    db_params = {
        "new_db": "inventory",
        "new_table": "products",
        "columns": ["ItemNumber","ItemName","Price"],
        "column_types": ["integer", "varchar(40)", "real"],
        "sql_script_fn": "insert.sql"
    }

    # Parameters for updating catalog.databases table
    catalog_db_params = {
        "dbid": 8,
        "engine_id": 0,
        "name": db_params['new_db'],
        "userid": "postgres",
        "password": "test"
    }

    # Parameters for updating catalog.objects table
    catalog_object_params = {
        "oid": 52,
        "name": db_params['new_table'],
        "fields": "ItemNumber,ItemName,Price",
        "logical_db": 8,
        "physical_db": 8
    }

    # Step 1: Create new database
    cc = CatalogClient(catalog_cred, database="postgres")
    cc.conn.autocommit = True  # Autocommit must be on for database creation
    cur = cc.conn.cursor()
    cur.execute("CREATE DATABASE " + db_params["new_db"] + ";")
    cur.close()
    print "Created new database"

    # Step 2: Create new table on the newly created database
    cc = CatalogClient(catalog_cred, database=db_params["new_db"])
    cur = cc.conn.cursor()
    columns = ", ".join([x[0] + " " + x[1] for x in zip(db_params['columns'], db_params['column_types'])])
    sql_str = "CREATE TABLE {} ({});".format(db_params['new_table'], columns)
    # sql_str = "CREATE TABLE products (ItemNumber integer,    ItemName varchar(40), Price real);"
    cur.execute(sql_str)
    cur.close()
    cc.conn.commit()
    print "Created new table"

    # Step 3: Insert data into new table from an existing sql script
    cc = CatalogClient(catalog_cred, database=db_params["new_db"])
    cc.exec_sql_script(db_params["sql_script_fn"])
    cc.conn.close()
    print "Inserted data into new table"

    # Step 4: Connect to the "bigdawg_catalog" database and update the "databases" table
    cc = CatalogClient(catalog_cred, database="bigdawg_catalog")
    cc.insert_database_info(catalog_db_params)

    # Step 5: Update the "objects" table
    cc.insert_object_info(catalog_object_params)
    cc.conn.close()

    # Step 6: Connect to the "bigdawg_schemas" database and create a new table
    cc = CatalogClient(catalog_cred, database="bigdawg_schemas")
    cur = cc.conn.cursor()
    cur.execute(sql_str)
    cur.close()
    cc.conn.commit()
    print "Updated bigdawg_schemas"