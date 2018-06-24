"""
Utilities for viewing Catalog contents for the BigDAWG Administrative Interface

See example usage at the bottom.
"""
import psycopg2

class CatalogClient:
    """
    Handles interaction with the Catalog database
    """
    def __init__(self, database="bigdawg_catalog", user="pguser", password="test", host="192.168.99.100", port="5400"):
        """
        Establishes connection to Catalog database

        :param database: database name
        :param user: username
        :param password: password
        :param host: hostname for database
        :param port: port
        """
        self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        self.host = host
        print ("Opened database successfully")

    def __del__(self):
        """
        Destructor to close the connection
        :return: null
        """
        self.conn.close()
        print ("Connection closed")

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

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

    def get_object(self, oid):
        """
        Reads objects table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT name, fields, logical_db, physical_db from catalog.objects where oid=" + str(oid))
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None

        return rows[0]

    def get_database(self, dbid):
        cur = self.conn.cursor()
        cur.execute("SELECT engine_id, name, userid, password from catalog.databases where dbid=" + str(dbid))
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None

        return rows[0]

    def get_engine(self, eid):
        cur = self.conn.cursor()
        cur.execute("SELECT name, host, port, connection_properties from catalog.engines where eid=" + str(eid))
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None
        return rows[0]

    def insert_engine(self, name, host, port, connection_properties):
        cur = self.conn.cursor()
        insertStatement = 'INSERT into catalog.engines (name, host, port, connection_properties) values (%s, %s, %s, %s)'
        values = [name, host, port, connection_properties]
        try:
            result = cur.execute(insertStatement, values)
        except psycopg2.Error as e:
            return str(e)
        self.conn.commit()
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

    def get_engines(self):
        """
        Reads engines table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.engines")
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_islands(self):
        """
        Reads islands table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.islands")
        rows = cur.fetchall()
        cur.close()
        return rows

if __name__ == "__main__":

    # Examples:
    cc = CatalogClient()  # get catalog client instance

    # Read objects table
    rows = cc.get_objects()
    for row in rows:
        print (row)

    # Read databases table
    rows = cc.get_databases()
    for row in rows:
        print (row)

    # Read islands table
    rows = cc.get_islands()
    for row in rows:
        print (row)
