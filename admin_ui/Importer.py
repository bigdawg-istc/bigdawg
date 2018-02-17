"""
Utilities for importing data

See example usage at the bottom.
"""
import json, numbers
import os
import psycopg2

class Importer:
    """
    Handles interaction with the Catalog databaseq
    """
    def __init__(self, catalog_client, schema_client):
        self.catalog_client = catalog_client
        self.schema_client = schema_client

    @staticmethod
    def error_msg(msg):
        msg = str(msg)
        result = "{ \"success\": false, \"error\": \"" + msg.replace('"', '\\"').replace("\n","\\n") + "\" }"
        print msg
        return result

    def import_data(self, data):
        data_import = json.loads(data)
        if not data_import:
            return Importer.error_msg("could not parse json")
        if not data_import.has_key('oid'):
            return Importer.error_msg("no oid in json")
        oid = data_import['oid']
        if not isinstance(oid, numbers.Number):
            return Importer.error_msg("oid is not a number")
        object_row = self.catalog_client.get_object(oid)
        if not object_row:
            return Importer.error_msg("could not find oid in database")

        fields = object_row[1]
        if not fields:
            return Importer.error_msg("no fields for object in database")

        if not isinstance(fields, str):
            return Importer.error_msg("fields is not a string")

        fields_split = fields.split(",")
        if not fields_split:
            return Importer.error_msg("unable to split fields: " + fields)

        fields_len = len(fields_split)

        name = object_row[0]
        if not name:
            return Importer.error_msg("unable to find name for this object: " + str(oid))

        if not isinstance(name, str):
            return Importer.error_msg("object name is not a string, oid: " + str(oid))

        name_parts = name.split('.')
        if not len(name_parts) == 2:
            return Importer.error_msg("object name is expected to be something.something, instead it's: " + name)

        table_schema = name_parts[0]
        table_name = name_parts[1]

        schema = self.schema_client.get_datatypes_for_table(table_schema, table_name)
        if not schema:
            return Importer.error_msg("could not lookup schema for table_schema: " + table_schema + ", table_name: " + table_name)

        data_types = {}
        for row in schema:
            if len(row) < 4:
                return Importer.error_msg("schema row length unexpected: " + str(len(row)))
            column_name = row[2]
            data_type = row[3]
            data_types[column_name] = data_type

        if not data_import.has_key('csv'):
            return Importer.error_msg("no csv in json")

        csv_data = data_import['csv']
        if not csv_data:
            return Importer.error_msg("csv data blank")

        if not isinstance(csv_data, list):
            return Importer.error_msg("csv data is not a list")

        row_num = 1
        inserts = []
        for row in csv_data:
            if not isinstance(row, list):
                return Importer.error_msg("csv data at row " + str(row_num) + " not a list")
            if len(row) != fields_len:
                return Importer.error_msg("csv data at row " + str(row_num) + " not right length (inital row length " + str(fields_len) + " but this row is length " + str(len(row)))

            insert_statement = "insert into " + table_schema + "." + table_name + "("
            values_statement = ""
            values = []
            for i in range(0, fields_len):
                cur_field = fields_split[i]
                if not data_types.has_key(cur_field):
                    return Importer.error_msg("not able to find data type for " + cur_field)
                if i != 0:
                    insert_statement += ","
                    values_statement += ","
                insert_statement += cur_field
                data_type = data_types[cur_field]
                if data_type == 'integer' and not isinstance(row[i], numbers.Number):
                    return Importer.error_msg("row " + row_num + " value " + str(i) + " expected to be instance of number, instead: " + str(row[i]))
                if data_type == 'double precision' and not isinstance(row[i], numbers.Number):
                    return Importer.error_msg("row " + row_num + " value " + str(i) + " expected to be instance of number, instead: " + str(row[i]))
                if data_type == 'string' and not isinstance(row[i], str):
                    return Importer.error_msg("row " + row_num + " value " + str(i) + " expected to be instance of string, instead: " + str(row[i]))

                values_statement += "%s"
                values.append(row[i])

            inserts.append({
                "statement": insert_statement + ") values (" + values_statement + ")",
                "values": values
            })
            row_num += 1
        return self.insert_data(object_row[3], inserts)

    def insert_data(self, dbid, inserts):
        database_row = self.catalog_client.get_database(dbid)
        if not database_row:
            return Importer.error_msg("Can't find database from dbid " + str(dbid))

        engine_id = database_row[0]
        database_name = database_row[1]
        user = database_row[2]
        password = database_row[3]

        if not engine_id:
            return Importer.error_msg("Unknown engine ID for dbid " + str(dbid))

        if not database_name:
            return Importer.error_msg("Can't lookup database name")

        if not isinstance(database_name, str):
            return Importer.error_msg("database name not a string")

        if not isinstance(user, str):
            return Importer.error_msg("database user not a string")

        if not isinstance(password, str):
            return Importer.error_msg("password not a string")

        engine_row = self.catalog_client.get_engine(engine_id)

        if not engine_row:
            return Importer.error_msg("Could not lookup engine " + str(engine_id))

        host = engine_row[1]
        port = engine_row[2]
        connection_properties = engine_row[3]

        if not isinstance(connection_properties, str):
            return Importer.error_msg("connection properties must be a string")

        if not connection_properties:
            return Importer.error_msg("unknown connection properties for engine: " + str(engine_id))

        if not connection_properties.startswith("PostgreSQL"):
            return Importer.error_msg("unknown engine type: " + str(connection_properties))

        if not port:
            return Importer.error_msg("unknown port for engine: " + str(engine_id))

        if not isinstance(host, str):
            return Importer.error_msg("host is not a string")

        # Hosts can be specified in the environment (or .env file) as "NAME_OF_SYSTEM" where [.-] becomes "_"
        #  (dot or dash becomes underscore)
        env_hostname_key = host.upper().replace(".","_").replace("-","_")
        local_hostname = os.environ.get(env_hostname_key) or self.catalog_client.host
        local_port = os.environ.get(env_hostname_key) or port

        try:
            conn = psycopg2.connect(database=database_name, user=user, password=password, host=local_hostname, port=local_port)
        except psycopg2.OperationalError as e:
            return Importer.error_msg("Unable to connect: " + str(e))

        cur = conn.cursor()
        for insert in inserts:
            try:
                cur.execute(insert['statement'], insert['values'])
            except psycopg2.Error as e:
                return Importer.error_msg(str(e))
            conn.commit()

        cur.close()
        conn.close()

        return "{ \"success\": true }"
