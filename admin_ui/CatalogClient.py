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

    def get_engine_by_name(self, name):
        return self.execute_single_row_select("SELECT eid, name, host, port, connection_properties from catalog.engines where name=%s", [name])

    def get_endpoint_by_engine_id_name(self, engine_id, name):
        return self.execute_single_row_select("SELECT dbid, engine_id, name, userid, password from catalog.databases where engine_id=%s and name=%s", [engine_id, name])

    def get_object_by_name_island_name(self, name, island_name):
        # This is adapted from CatalogViewer.java
        wherePred = "(o.name ilike PERCENT_REPLACE_ME%sPERCENT_REPLACE_ME and i.scope_name ilike PERCENT_REPLACE_ME%sPERCENT_REPLACE_ME) "
        selectStatement = "select o.name obj, d1.name src_db, d2.name dst_db, i.scope_name island, c.access_method "\
                          + "from catalog.objects o " + "join catalog.databases d1 	on o.physical_db = d1.dbid "\
                          + "join catalog.engines e1 		on d1.engine_id = e1.eid "\
                          + "join catalog.casts c 		on e1.eid = c.src_eid "\
                          + "join catalog.engines e2 		on c.dst_eid = e2.eid "\
                          + "join catalog.databases d2 	on d2.engine_id = e2.eid "\
                          + "join catalog.shims sh 		on e2.eid = sh.engine_id "\
                          + "join catalog.islands i 		on sh.island_id = i.iid " + "where " + wherePred\
                          + "order by o.name, d1.name, d2.name, i.scope_name;"
        cur = self.conn.cursor()


        return self.execute_single_row_select(selectStatement, [name.replace("%", "\\%"), island_name])

    def execute_single_row_select(self, statement, values):
        cur = self.conn.cursor()
        statement = cur.mogrify(statement, values)
        statement = statement.replace('PERCENT_REPLACE_ME\'', '\'%')
        statement = statement.replace('\'PERCENT_REPLACE_ME', '%\'')
        print statement
        cur.execute(statement)
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None
        return rows[0]

    def insert_engine(self, name, host, port, connection_properties):
        values = [name, host, port, connection_properties]
        return self.execute_insert_with_max('catalog.engines', 'eid', ['name', 'host', 'port', 'connection_properties'], values)

    def insert_database(self, engine_id, name, userid, password):
        values = [engine_id, name, userid, password]
        return self.execute_insert_with_max('catalog.databases', 'dbid', ['engine_id', 'name', 'userid', 'password'], values)

    def insert_object(self, name, fields, logical_db, physical_db):
        values = [name, fields, logical_db, physical_db]
        return self.execute_insert_with_max('catalog.objects', 'oid', ['name','fields','logical_db','physical_db'], values)

    def getMaxId(self, table, column):
        cur = self.conn.cursor()
        maxStatement = 'SELECT max(' + column + ') m from ' + table
        cur.execute(maxStatement)
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return 0
        return rows[0][0]

    def execute_seq_update(self, seq, value):
        seqStatement = "SELECT setval('" + seq + "'::regclass, " + str(value) + ")"
        cur = self.conn.cursor()
        cur.execute(seqStatement)
        cur.close()

    def execute_insert_with_max(self, table, idColumn, fields, values):
        assert(len(fields) == len(values))
        id = self.getMaxId(table, idColumn)
        id += 1
        insertStatement = "INSERT into " + table + "(" + idColumn
        valuesStatement = "%s"
        finalValues = [id]
        for i in range(len(fields)):
            insertStatement += ', '
            valuesStatement += ', %s'
            insertStatement += fields[i]
            finalValues.append(values[i])

        insertStatement += ') values (' + valuesStatement + ')'
        result = self.execute_insert(insertStatement, finalValues)
        if result is True:
            self.execute_seq_update(table + '_' + idColumn + '_seq', id)
            return id
        return result

    def execute_insert(self, insertStatement, values):
        cur = self.conn.cursor()
        try:
            cur.execute(insertStatement, values)
            self.conn.commit()
            cur.close()
            return True
        except psycopg2.Error as e:
            return str(e)

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
