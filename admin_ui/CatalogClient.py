"""
Utilities for viewing Catalog contents for the BigDAWG Administrative Interface

See example usage at the bottom.
"""
import psycopg2


class CatalogClient:
    """
    Handles interaction with the Catalog database
    """
    def __init__(self, database="bigdawg_catalog", user="pguser", password="test", host="saw.csail.mit.edu", port="5400"):
        """
        Establishes connection to Catalog database

        :param database: database name
        :param user: username
        :param password: password
        :param host: hostname for database
        :param port: port
        """
        self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        print "Opened database successfully"

    def __del__(self):
        """
        Destructor to close the connection
        :return: null
        """
        self.conn.close()
        print "Connection closed"

    def get_objects(self):
        """
        Reads objects table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.objects")
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_databases(self):
        """
        Reads databases table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.databases")
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_islands(self):
        """
        Reads islands table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.databases")
        rows = cur.fetchall()
        cur.close()
        return rows


# Examples:
cc = CatalogClient()  # get catalog client instance

# Read objects table
rows = cc.get_objects()
for row in rows:
    print row

# Read databases table
rows = cc.get_databases()
for row in rows:
    print row

# Read islands table
rows = cc.get_islands()
for row in rows:
    print row
