"""
Utilities for importing data

See example usage at the bottom.
"""
import json, numbers
import os
import psycopg2
import mysql.connector
from Util import Util

class Importer:
    """
    Handles interaction with the Catalog databaseq
    """
    def __init__(self, catalog_client, schema_client):
        self.catalog_client = catalog_client
        self.schema_client = schema_client

    def get_schemas(self, data):
        data_import = json.loads(data)
        if not data_import:
            return Util.error_msg("could not parse json")

        if not data_import.has_key('dbid'):
            return Util.error_msg("No dbid passed in")

        dbid = data_import['dbid']
        if not isinstance(dbid, numbers.Number):
            return Util.error_msg("dbid is not a number")

        (connection_properties, host, port, database_name, user, password) = self.get_connection_info(dbid)
        conn = self.get_connection_postgres(host, port, database_name, user, password)
        if isinstance(conn, str): # should be an error message then
            return conn
        try:
            if isinstance(conn, basestring):
                return conn
        except NameError:
            pass

        cur = conn.cursor()
        cur.execute("SELECT schema_name from information_schema.schemata where schema_name not like 'pg_%' and schema_name != 'information_schema'");
        rows = cur.fetchall()
        cur.close()

        schemas = []
        for row in rows:
            schemas.append(row[0])

        return json.JSONEncoder().encode({ "success": True, "schemas": schemas })



    def create_table(self, data):
        if not data.has_key('dbid'):
            return Util.error_msg("No dbid in request")

        dbid = data['dbid']
        if not isinstance(dbid, numbers.Number):
            return Util.error_msg("dbid is not a number")

        if not data.has_key('fields_name'):
            return Util.error_msg("fields_name needs to be provided")

        fieldsName = data['fields_name']

        if not data.has_key('fields'):
            return Util.error_msg("expecting fields to be passed in")

        fields = data['fields']

        database_row = self.catalog_client.get_database(dbid)
        if not database_row:
            return Util.error_msg("Could not find database for dbid: " + str(dbid))

        oid = self.catalog_client.insert_object(fieldsName, ','.join(fields), dbid, dbid)
        if not isinstance(oid, int):
            return Util.error_msg(oid)

        if data.has_key('schema'):
            schema = data['schema']
            result = self.schema_client.execute_statement(schema, None)
            if result != True:
                return Util.error_msg("Problem creating schema: " + str(result))

        # Postgres create table
        create = data['create']
        (connection_properties, host, port, database_name, user, password) = self.get_connection_info(dbid)

        if (connection_properties.startswith("PostgreSQL")):
            conn = self.get_connection_postgres(host, port, database_name, user, password)
        elif (connection_properties.startswith("MySQL")):
            conn = self.get_connection_mysql(host, port, database_name, user, password)
        else:
            return Util.error_msg("Unsupported engine")

        if self.check_result_str(conn):
            return conn

        cur = conn.cursor()
        try:
            cur.execute(create)
            conn.commit()
            cur.close()
            conn.close()

        except psycopg2.Error as e:
            return Util.error_msg(str(e))
        except mysql.connector.Error as e:
            return Util.error_msg(str(e))

        return oid

    def check_result_str(self, result):
        if isinstance(result, str):  # should be an error message then
            return True
        try:
            if isinstance(result, basestring):
                return True
        except NameError:
            pass

        return False

    def import_data(self, data):
        data_import = json.loads(data)
        if not data_import:
            return Util.error_msg("could not parse json")

        if not data_import.has_key('oid'):
            oid = self.create_table(data_import)
            if not isinstance(oid, int):
                return oid
        else:
            oid = data_import['oid']

        if not isinstance(oid, numbers.Number):
            return Util.error_msg("oid is not a number")

        object_row = self.catalog_client.get_object(oid)
        if not object_row:
            return Util.error_msg("could not find oid in database")

        fields = object_row[2]
        if not fields:
            return Util.error_msg("no fields for object in database")

        if not isinstance(fields, str):
            return Util.error_msg("fields is not a string")

        fields_split = fields.split(",")
        if not fields_split:
            return Util.error_msg("unable to split fields: " + fields)

        fields_len = len(fields_split)

        name = object_row[1]
        if not name:
            return Util.error_msg("unable to find name for this object: " + str(oid))

        if not isinstance(name, str):
            return Util.error_msg("object name is not a string, oid: " + str(oid))

        name_parts = name.split('.')
        if not len(name_parts) == 2:
            table_name = name_parts[0]
            table_schema = 'public'
        else:
            table_schema = name_parts[0]
            table_name = name_parts[1]

        schema = self.schema_client.get_datatypes_for_table(table_schema, table_name)
        if not schema:
            return Util.error_msg("could not lookup schema for table_schema: " + table_schema + ", table_name: " + table_name)

        data_types = {}
        for row in schema:
            if len(row) < 4:
                return Util.error_msg("schema row length unexpected: " + str(len(row)))
            column_name = row[2]
            data_type = row[3]
            data_types[column_name] = data_type

        if not data_import.has_key('csv'):
            return Util.error_msg("no csv in json")

        csv_data = data_import['csv']
        if not csv_data:
            return Util.error_msg("csv data blank")

        if not isinstance(csv_data, list):
            return Util.error_msg("csv data is not a list")

        row_num = 1
        inserts = []
        for row in csv_data:
            if not isinstance(row, list):
                return Util.error_msg("csv data at row " + str(row_num) + " not a list")
            if len(row) != fields_len:
                return Util.error_msg("csv data at row " + str(row_num) + " not right length (inital row length " + str(fields_len) + " but this row is length " + str(len(row)))

            insert_statement = "insert into " + name + "("
            values_statement = ""
            values = []
            for i in range(0, fields_len):
                cur_field = fields_split[i]
                if not data_types.has_key(cur_field):
                    return Util.error_msg("not able to find data type for " + cur_field)
                if i != 0:
                    insert_statement += ","
                    values_statement += ","
                insert_statement += cur_field
                data_type = data_types[cur_field]
                if data_type == 'integer' and not isinstance(row[i], numbers.Number):
                    return Util.error_msg("row " + str(row_num) + " value " + str(i) + " expected to be instance of number, instead: " + str(row[i]))
                if data_type == 'double precision' and not isinstance(row[i], numbers.Number):
                    return Util.error_msg("row " + str(row_num) + " value " + str(i) + " expected to be instance of number, instead: " + str(row[i]))
                if data_type == 'string' and not isinstance(row[i], str):
                    return Util.error_msg("row " + str(row_num) + " value " + str(i) + " expected to be instance of string, instead: " + str(row[i]))

                values_statement += "%s"
                values.append(row[i])

            inserts.append({
                "statement": insert_statement + ") values (" + values_statement + ")",
                "values": values
            })
            row_num += 1
        return self.insert_data(object_row[4], inserts)

    def get_connection_info(self, dbid):
        database_row = self.catalog_client.get_database(dbid)
        if not database_row:
            return Util.error_msg("Can't find database from dbid " + str(dbid))

        engine_id = database_row[1]
        database_name = database_row[2]
        user = database_row[3]
        password = database_row[4]

        if not engine_id:
            return Util.error_msg("Unknown engine ID for dbid " + str(dbid))

        if not database_name:
            return Util.error_msg("Can't lookup database name")

        if not isinstance(database_name, str):
            return Util.error_msg("database name not a string")

        if not isinstance(user, str):
            return Util.error_msg("database user not a string")

        if not isinstance(password, str):
            return Util.error_msg("password not a string")

        engine_row = self.catalog_client.get_engine(engine_id)

        if not engine_row:
            return Util.error_msg("Could not lookup engine " + str(engine_id))

        host = engine_row[2]
        port = engine_row[3]
        connection_properties = engine_row[4]

        if not isinstance(connection_properties, str):
            return Util.error_msg("connection properties must be a string")

        if not connection_properties:
            return Util.error_msg("unknown connection properties for engine: " + str(engine_id))

        if not port:
            return Util.error_msg("unknown port for engine: " + str(engine_id))

        if not isinstance(host, str):
            return Util.error_msg("host is not a string")

        # Hosts can be specified in the environment (or .env file) as "NAME_OF_SYSTEM" where [.-] becomes "_"
        #  (dot or dash becomes underscore)
        env_hostname_key = host.upper().replace(".","_").replace("-","_")
        local_hostname = os.environ.get(env_hostname_key) or self.catalog_client.host
        local_port = os.environ.get(env_hostname_key) or port

        return (connection_properties, local_hostname, local_port, database_name, user, password)

    def get_connection_postgres(self, host, port, database_name, user, password):
        print "Importer: connecting to: " + database_name + ", " + user + ", " + host + ", " + str(port)
        try:
            conn = psycopg2.connect(database=database_name, user=user, password=password, host=host, port=port)
        except psycopg2.OperationalError as e:
            return Util.error_msg("Unable to connect: " + str(e))

        return conn

    def get_connection_mysql(self, host, port, database_name, user, password):
        try:
            conn = mysql.connector.connect(user=user, password=password, host=host, port=port, database=database_name)
        except mysql.connector.Error as e:
            return Util.error_msg("Could not connect: " + str(e))

        return conn

    def insert_data(self, dbid, inserts):
        (connection_properties, host, port, database_name, user, password) = self.get_connection_info(dbid)
        print "Importer: connecting to: " + database_name + ", " + user + ", " + host + ", " + str(port)

        if connection_properties.startswith("PostgreSQL"):
            conn = self.get_connection_postgres(host, port, database_name, user, password)
        elif connection_properties.startswith("MySQL"):
            conn = self.get_connection_mysql(host, port, database_name, user, password)
        else:
            return Util.error_msg("Unsupported engine.")

        if self.check_result_str(conn):
            return conn

        cur = conn.cursor()
        for insert in inserts:
            try:
                print insert['statement']
                print insert['values']
                cur.execute(insert['statement'], insert['values'])
                conn.commit()
            except psycopg2.Error as e:
                return Util.error_msg(str(e))
            except mysql.connector.Error as e:
                return Util.error_msg(str(e))

        cur.close()
        conn.close()

        return Util.success_msg()
