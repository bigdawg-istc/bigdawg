"""
Utilities for importing data

See example usage at the bottom.
"""
from CatalogClient import CatalogClient
from SchemaClient import SchemaClient
import json, numbers, list

class Importer:
    """
    Handles interaction with the Catalog databaseq
    """
    def __init__(self, catalog_client, schema_client):
        self.catalog_client = catalog_client
        self.schema_client = schema_client

    @staticmethod
    def error_msg(msg):
        result = "{ success: false, error: \"" + msg + "\" }"
        print msg
        return result

    def import_data(self, data):
        data_import = json.loads(data)
        if not data_import:
            return Klass.error_msg("could not parse json")
        if not data_import.has_key('oid'):
            return Klass.error_msg("no oid in json")
        oid = data_import['oid']
        if not isinstance(oid, numbers.Number):
            return Klass.error_msg("oid is not a number")
        object_row = self.catalog_client.get_object(oid)
        if not object_row:
            return Klass.error_msg("could not find oid in database")

        fields = object_row[2]
        if not fields:
            return Klass.error_msg("no fields for object in database")

        if not isinstance(fields, str):
            return Klass.error_msg("fields is not a string")

        fields_split = fields.split(",")
        if not fields_split:
            return Klass.error_msg("unable to split fields: " + fields)

        fields_len = len(fields_split)

        name = object_row[1]
        if not name:
            return Klass.error_msg("unable to find name for this object: " + str(oid))

        if not isinstance(name, str):
            return Klass.error_msg("object name is not a string, oid: " + str(oid))

        name_parts = name.split('.')
        if not len(name_parts) == 2:
            return Klass.error_msg("object name is expected to be something.something, instead it's: " + name)

        table_schema = name_parts[0]
        table_name = name_parts[1]

        schema = self.schema_client.get_datatypes_for_table(table_schema, table_name)
        if not schema:
            return Klass.error_msg("could not lookup schema for table_schema: " + table_schema + ", table_name: " + table_name)

        data_types = {}
        for row in schema:
            if len(row) < 4:
                return Klass.error_msg("schema row length unexpected: " + str(len(row)))
            column_name = row[2]
            data_type = row[3]
            data_types[column_name] = data_type

        if not data_import.has_key('csv'):
            return Klass.error_msg("no csv in json")

        csv_data = data_import['csv']
        if not csv_data:
            return Klass.error_msg("csv data blank")

        if not isinstance(csv_data, list):
            return Klass.error_msg("csv data is not a list")

        row_num = 1
        inserts = []
        for row in csv_data:
            if not isinstance(row, list):
                return Klass.error_msg("csv data at row " + str(row_num) + " not a list")
            if len(row) != fields_len:
                return Klass.error_msg("csv data at row " + str(row_num) + " not right length (inital row length " + str(fields_len) + " but this row is length " + str(len(row)))

            insert_statement = "insert into " + table_schema + "." + table_name + "("
            values = ""
            for i in range(0, fields_len):
                cur_field = fields_split[i]
                if not data_types.has_key(cur_field):
                    return Klass.error_msg("not able to find data type for " + cur_field)
                if i != 0:
                    insert_statement += ","
                    values += ","
                insert_statement += cur_field
                data_type = data_types[cur_field]
                if data_type == 'integer' or data_type == 'double precision':
                    values += row[i]
                else:
                    values += "'" + row[i].replace("'", "''") + "'"
            inserts.append(insert_statement + ") values (" + values + ")")

        print inserts[0]