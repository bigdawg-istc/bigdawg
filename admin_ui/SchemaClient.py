"""
Utilities for viewing Schema contents for the BigDAWG Administrative Interface

See example usage at the bottom.
"""
import psycopg2


class SchemaClient:
    """
    Handles interaction with the Catalog databaseq
    """
    def __init__(self, database="bigdawg_schema", user="pguser", password="test", host="192.168.99.100", port="5400"):
        """
        Establishes connection to Catalog database

        :param database: database name
        :param user: username
        :param password: password
        :param host: hostname for database
        :param port: port
        """
        self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        print ("Opened database successfully")

    def __del__(self):
        """
        Destructor to close the connection
        :return: null
        """
        self.conn.close()
        print ("Connection closed")

    def get_datatypes(self):
        """
        Reads objects table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT table_schema, table_name, column_name, data_type from information_schema.columns where table_schema not in ('information_schema', 'pg_catalog')")
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_datatypes_for_table(self, table_schema, table_name):
        """
        Reads objects table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT table_schema, table_name, column_name, data_type from information_schema.columns where table_schema = '" + table_schema + "' and table_name = '" + table_name + "'")
        rows = cur.fetchall()
        cur.close()
        return rows

    def execute_statement(self, statement, values):
        cur = self.conn.cursor()
        try:
            cur.execute(statement, values)
            self.conn.commit()
            cur.close()
            return True
        except psycopg2.Error as e:
            return str(e)

if __name__ == "__main__":

    # Examples:
    cc = SchemaClient()  # get catalog client instance

    # Read infomation_schema.columns table
    rows = cc.get_datatypes()
    for row in rows:
        print (row)
