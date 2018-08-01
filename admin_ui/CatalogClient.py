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
        cur.execute("SELECT oid, name, fields, logical_db, physical_db from catalog.objects where oid=%s", [int(oid)])
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None

        return rows[0]

    def get_objects_by_phsyical_db(self, physical_db):
        """
        Reads objects table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT oid, name, fields, logical_db, physical_db from catalog.objects where physical_db=%s",[int(physical_db)])
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None

        return rows


    def get_database(self, dbid):
        cur = self.conn.cursor()
        cur.execute("SELECT dbid, engine_id, name, userid, password from catalog.databases where dbid=%s",[int(dbid)])
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None

        return rows[0]

    def get_engine(self, eid):
        cur = self.conn.cursor()
        cur.execute("SELECT eid, name, host, port, connection_properties from catalog.engines where eid=%s",[int(eid)])
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None
        return rows[0]

    def get_engine_by_name(self, name):
        return self.execute_single_row_select("SELECT eid, name, host, port, connection_properties from catalog.engines where name=%s", [name])

    def get_database_by_engine_id_name(self, engine_id, name):
        return self.execute_single_row_select("SELECT dbid, engine_id, name, userid, password from catalog.databases where engine_id=%s and name=%s", [int(engine_id), name])

    def get_island_by_scope_name(self, scope_name):
        return self.execute_single_row_select("SELECT iid, scope_name, access_method from catalog.islands where scope_name=%s", [scope_name])

    def get_object_by_name_island_name(self, name, island_name):
        # This is adapted from CatalogViewer.java
        wherePred = "lower(o.name) = lower(%s)"
        selectStatement = "select o.oid, o.name, o.fields, o.logical_db, o.physical_db "\
                          + "from catalog.objects o "\
                          + "join catalog.databases d on o.physical_db = d.dbid "\
                          + "join catalog.shims s on d.engine_id = s.engine_id "\
                          + "join catalog.islands i on s.island_id = i.iid where " + wherePred \
                          + " AND scope_name = %s"
        return self.execute_single_row_select(selectStatement, [name, island_name])


    def execute_single_row_select(self, statement, values):
        cur = self.conn.cursor()
        cur.execute(statement, values)
        rows = cur.fetchall()
        cur.close()
        if not rows:
            return None
        return rows[0]

    def update_engine(self, eid, name, host, port, connection_properties):
        updateStatement = "update catalog.engines set name=%s, host=%s, port=%s, connection_properties=%s where eid=%s"
        values = [name, host, port, connection_properties, eid]
        return self.execute_update(updateStatement, values)

    def update_database(self, dbid, engine_id, name, userid, password):
        updateStatement = "update catalog.databases set engine_id=%s, name=%s, userid=%s, password=%s where dbid=%s"
        values = [engine_id, name, userid, password, dbid]
        return self.execute_update(updateStatement, values)

    def update_object(self, oid, name, fields, logical_db, physical_db):
        updateStatement = "update catalog.objects set name=%s, fields=%s, logical_db=%s, physical_db=%s where oid=%s"
        values = [name, fields, logical_db, physical_db, oid]
        return self.execute_update(updateStatement, values)

    def execute_update(self, updateStatement, values):
        cur = self.conn.cursor()
        try:
            cur.execute(updateStatement, values)
            self.conn.commit()
            cur.close()
            return True
        except psycopg2.Error as e:
            return str(e)

    def insert_engine(self, name, host, port, connection_properties):
        values = [name, host, port, connection_properties]
        return self.execute_insert_with_max('catalog.engines', 'eid', ['name', 'host', 'port', 'connection_properties'], values)

    def insert_shim(self, island_id, engine_id):
        values = [island_id, engine_id, 'N/A']
        return self.execute_insert_with_max('catalog.shims', 'shim_id', ['island_id', 'engine_id', 'access_method'], values)

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

    def delete_shim(self, island_id, engine_id):
        statement = "DELETE FROM catalog.shims where island_id = %s and engine_id = %s"
        return self.execute_statement(statement, [island_id, engine_id])

    def delete_object(self, oid):
        statement = "DELETE FROM catalog.objects where oid = %s"
        return self.execute_statement(statement, [oid])

    def delete_database(self, dbid):
        statement = "DELETE FROM catalog.databases where dbid = %s"
        return self.execute_statement(statement, [dbid])

    def delete_engine(self, eid):
        statement = "DELETE FROM catalog.engines where eid = %s"
        return self.execute_statement(statement, [eid])



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
        result = self.execute_statement(insertStatement, finalValues)
        if result is True:
            self.execute_seq_update(table + '_' + idColumn + '_seq', id)
            return id
        return result

    def execute_statement(self, insertStatement, values):
        cur = self.conn.cursor()
        try:
            cur.execute(insertStatement, values)
            self.conn.commit()
            cur.close()
            return True
        except psycopg2.Error as e:
            return str(e)

    def get_databases_by_island(self, scope_name):
        return self.get_databases_operator_island(scope_name, "=")

    def get_databases_excluding_island(self, scope_name):
        return self.get_databases_operator_island(scope_name, "!=")

    def get_databases_operator_island(self, scope_name, operator):
        """
        Reads databases table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.databases d join catalog.engines e on d.engine_id = e.eid join catalog.shims s on s.engine_id = e.eid join catalog.islands i on i.iid = s.island_id where i.scope_name " + operator + " %s order by d.engine_id ASC", [scope_name])
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_databases_by_engine_id(self, engine_id):
        """
        Reads databases table and returns all rows.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.databases where engine_id = %s order by dbid ASC", [engine_id])
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_engines_by_island(self, scope_name):
        return self.get_engines_operator_island(scope_name, "=")

    def get_engines_operator_island(self, scope_name, operator):
        """
        Reads databases table and returns all rows that are either in the island scope_name or not.
        :return: rows: an iterable of tuples, where each element is a value of the row
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * from catalog.engines e join catalog.shims s on s.engine_id = e.eid join catalog.islands i on i.iid = s.island_id where i.scope_name " + operator + " %s order by e.eid ASC", [scope_name])
        rows = cur.fetchall()
        cur.close()
        return rows

    def get_engines_excluding_island(self, scope_name):
        return self.get_engines_operator_island(scope_name, "!=")

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
