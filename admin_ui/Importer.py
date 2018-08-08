"""
Utilities for importing data

See example usage at the bottom.
"""
import json, numbers
import os
import psycopg2
import mysql.connector
import re


try:
    import vertica_python
except:
    pass

try:
    import pyaccumulo
except:
    pass

from Util import Util

supportedTypes = ['PostgreSQL', 'MySQL', 'Vertica']

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

        result = self.get_connection_info(dbid)
        if isinstance(result, str): # should be an error message then
            return result
        try:
            if isinstance(result, basestring):
                return result
        except NameError:
            pass

        (connection_properties, host, port, databaseName, user, password) = result
        if connection_properties.startswith("Vertica"):
            conn = self.get_connection_vertica(host, port, databaseName, user, password)
            statement = "SELECT schema_name from v_catalog.schemata where schema_name not like 'v_%' and schema_name != 'TxtIndex'"
        else:
            conn = self.get_connection_postgres(host, port, databaseName, user, password)
            statement = "SELECT schema_name from information_schema.schemata where schema_name not like 'pg_%' and schema_name != 'information_schema'"
        if isinstance(conn, str): # should be an error message then
            return conn
        try:
            if isinstance(conn, basestring):
                return conn
        except NameError:
            pass

        cur = conn.cursor()
        cur.execute(statement)
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

        databaseRow = self.catalog_client.get_database(dbid)
        if not databaseRow:
            return Util.error_msg("Could not find database for dbid: " + str(dbid))

        oid = self.catalog_client.insert_object(fieldsName, ','.join(fields), dbid, dbid)
        if not isinstance(oid, int):
            return Util.error_msg(oid)

        (connection_properties, host, port, databaseName, user, password) = self.get_connection_info(dbid)
        supported = False
        for supportedType in supportedTypes:
            if (connection_properties.startswith(supportedType)):
                supported = True

        if not supported:
            return Util.error_msg("Engine not supported: " + connection_properties)

        # @TODO - fix accumulo driver issue so this will work, also find a python3 equivalent
        if connection_properties.startswith("Accumulo"):
            conn = self.get_connection_accumulo(host, port, user, password)
            tableName = data['fields_name']
            conn.create_table(tableName)
            return oid

        if data.has_key('schema'):
            schema = data['schema']
            result = self.schema_client.execute_statement(schema, None)
            if result != True:
                if (data.has_key('schema_name')):
                    createSchemaContainer = "create schema " + data['schema_name']
                    result = self.schema_client.execute_statement(createSchemaContainer, None)
                    if result != True:
                        return Util.error_msg("Problem creating schema (1): " + str(result))
                    else:
                        result = self.schema_client.execute_statement(schema, None)
                        if result != True:
                            return Util.error_msg("Problem creating schema (2): " + str(result))
                else:
                    return Util.error_msg("Problem creating schema (2): " + str(result))



        # Postgres create table
        create = data['create']

        if (connection_properties.startswith("PostgreSQL")):
            conn = self.get_connection_postgres(host, port, databaseName, user, password)
        elif (connection_properties.startswith("MySQL")):
            conn = self.get_connection_mysql(host, port, databaseName, user, password)
        elif (connection_properties.startswith("Vertica")):
            conn = self.get_connection_vertica(host, port, databaseName, user, password)
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
        dataImport = json.loads(data)
        if not dataImport:
            return Util.error_msg("could not parse json")

        if not dataImport.has_key('oid'):
            oid = self.create_table(dataImport)
            if not isinstance(oid, int):
                return oid
        else:
            oid = dataImport['oid']

        if not isinstance(oid, numbers.Number):
            return Util.error_msg("oid is not a number")

        object_row = self.catalog_client.get_object(oid)
        if not object_row:
            return Util.error_msg("could not find oid in database")

        dbid = object_row[4]
        if dataImport.has_key('dbid'):
            dbid = dataImport['dbid']

        (connection_properties, host, port, databaseName, user, password) = self.get_connection_info(dbid)
        if (connection_properties.startswith("Accumulo")):
            return Util.error_msg("Accumulo not yet supported")

        fields = object_row[2]
        if not fields:
            return Util.error_msg("no fields for object in database")

        if not isinstance(fields, str):
            return Util.error_msg("fields is not a string")

        fieldsSplit = fields.split(",")
        if not fieldsSplit:
            return Util.error_msg("unable to split fields: " + fields)

        fieldsLen = len(fieldsSplit)

        name = object_row[1]
        if not name:
            return Util.error_msg("unable to find name for this object: " + str(oid))

        if not isinstance(name, str):
            return Util.error_msg("object name is not a string, oid: " + str(oid))

        nameParts = name.split('.')
        if not len(nameParts) == 2:
            table_name = nameParts[0]
            table_schema = 'public'
        else:
            table_schema = nameParts[0]
            table_name = nameParts[1]

        schema = self.schema_client.get_datatypes_for_table(table_schema, table_name)
        if not schema:
            return Util.error_msg("could not lookup schema for table_schema: " + table_schema + ", table_name: " + table_name)

        dataTypes = {}
        for row in schema:
            if len(row) < 4:
                return Util.error_msg("schema row length unexpected: " + str(len(row)))
            column_name = row[2]
            dataType = row[3]
            dataTypes[column_name] = dataType

        if not dataImport.has_key('csv'):
            return Util.error_msg("no csv in json")

        csvData = dataImport['csv']
        if not csvData:
            return Util.error_msg("csv data blank")

        if not isinstance(csvData, list):
            return Util.error_msg("csv data is not a list")

        if (connection_properties.startswith("Accumulo")):
            return self.insert_data_accumulo(csvData, fieldsSplit, name, host, port, user, password)

        rowNum = 1
        inserts = []
        for row in csvData:
            if not isinstance(row, list):
                return Util.error_msg("csv data at row " + str(rowNum) + " not a list")
            if len(row) != fieldsLen:
                return Util.error_msg("csv data at row " + str(rowNum) + " not right length (inital row length " + str(fieldsLen) + " but this row is length " + str(len(row)))

            insertStatement = "insert into " + name + "("
            valuesStatement = ""
            values = []
            for i in range(0, fieldsLen):
                curField = fieldsSplit[i]
                if not dataTypes.has_key(curField):
                    return Util.error_msg("not able to find data type for " + curField)
                if i != 0:
                    insertStatement += ","
                    valuesStatement += ","
                insertStatement += curField
                dataType = dataTypes[curField]
                numberRe = re.compile('^-?[0-9]+$')
                doubleRe = re.compile('^-?[0-9]+\.?[0-9]*$')
                if dataType == 'integer' and not numberRe.match(str(row[i])):
                    return Util.error_msg("row " + str(rowNum) + " value " + str(i) + " expected to be instance of number, instead: " + str(row[i]))
                if dataType == 'double precision' and not doubleRe.match(str(row[i])):
                    return Util.error_msg("row " + str(rowNum) + " value " + str(i) + " expected to be instance of number, instead: " + str(row[i]))
                if dataType == 'string' and not isinstance(row[i], str):
                    return Util.error_msg("row " + str(rowNum) + " value " + str(i) + " expected to be instance of string, instead: " + str(row[i]))

                valuesStatement += "%s"
                values.append(row[i])

            inserts.append({
                "statement": insertStatement + ") values (" + valuesStatement + ")",
                "values": values
            })
            rowNum += 1
        return self.insert_data(object_row[4], inserts)

    def insert_data_accumulo(self, csvData, fields, name, host, port, user, password):
        conn = self.get_connection_accumulo(host, port, user, password)
        if not conn.table_exists(name):
            return Util.error_msg("Accumulo table " + name + " does not exist")

        wr = conn.create_batch_writer(name)
        fieldsLen = len(fields)
        rowNum = 1
        for row in csvData:
            m = pyaccumulo.Mutation(str(rowNum))
            for i in range(0, fieldsLen):
                field = fields[i]
                m.put(str(i + 1), field, str(row[i]))
            wr.add_mutation(m)
            rowNum += 1

        wr.close()
        return Util.success_msg()

    def get_connection_info(self, dbid):
        databaseRow = self.catalog_client.get_database(dbid)
        if not databaseRow:
            return Util.error_msg("Can't find database from dbid " + str(dbid))

        engineId = databaseRow[1]
        databaseName = databaseRow[2]
        user = databaseRow[3]
        password = databaseRow[4]

        if not engineId:
            return Util.error_msg("Unknown engine ID for dbid " + str(dbid))

        if not databaseName:
            return Util.error_msg("Can't lookup database name")

        if not isinstance(databaseName, str):
            return Util.error_msg("database name not a string")

        if not isinstance(user, str):
            return Util.error_msg("database user not a string")

        if not isinstance(password, str):
            return Util.error_msg("password not a string")

        engineRow = self.catalog_client.get_engine(engineId)

        if not engineRow:
            return Util.error_msg("Could not lookup engine " + str(engineId))

        host = engineRow[2]
        port = engineRow[3]
        connection_properties = engineRow[4]

        if not isinstance(connection_properties, str):
            return Util.error_msg("connection properties must be a string")

        if not connection_properties:
            return Util.error_msg("unknown connection properties for engine: " + str(engineId))

        if not port:
            return Util.error_msg("unknown port for engine: " + str(engineId))

        if not isinstance(host, str):
            return Util.error_msg("host is not a string")

        # Hosts can be specified in the environment (or .env file) as "NAME_OF_SYSTEM" where [.-] becomes "_"
        #  (dot or dash becomes underscore)
        envHostnameKey = host.upper().replace(".","_").replace("-","_")
        localHostname = os.environ.get(envHostnameKey) or self.catalog_client.host
        localPort = os.environ.get(envHostnameKey) or port

        return (connection_properties, localHostname, localPort, databaseName, user, password)

    def get_connection_postgres(self, host, port, databaseName, user, password):
        print "Importer: connecting to: " + databaseName + ", " + user + ", " + host + ", " + str(port)
        try:
            conn = psycopg2.connect(database=databaseName, user=user, password=password, host=host, port=port)
        except psycopg2.OperationalError as e:
            return Util.error_msg("Unable to connect: " + str(e))

        return conn

    def get_connection_vertica(self, host, port, databaseName, user, password):
        print "Importer: connecting to: " + databaseName + ", " + user + ", " + host + ", " + str(port)
        try:
            conn = vertica_python.connect(database=databaseName, user=user, password=password, host=host, port=port)
        except vertica_python.OperationalError as e:
            return Util.error_msg("Unable to connect: " + str(e))

        return conn

    def get_connection_mysql(self, host, port, databaseName, user, password):
        try:
            conn = mysql.connector.connect(user=user, password=password, host=host, port=port, database=databaseName)
        except mysql.connector.Error as e:
            return Util.error_msg("Could not connect: " + str(e))

        return conn

    def get_connection_accumulo(self, host, port, user, password):
        conn = pyaccumulo.Accumulo(host=host, port=port, user=user, password=password)
        return conn


    def insert_data(self, dbid, inserts):
        (connection_properties, host, port, databaseName, user, password) = self.get_connection_info(dbid)
        print "Importer: connecting to: " + databaseName + ", " + user + ", " + host + ", " + str(port)

        if connection_properties.startswith("PostgreSQL"):
            conn = self.get_connection_postgres(host, port, databaseName, user, password)
        elif connection_properties.startswith("MySQL"):
            conn = self.get_connection_mysql(host, port, databaseName, user, password)
        elif connection_properties.startswith("Vertica"):
            conn = self.get_connection_vertica(host, port, databaseName, user, password)
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
