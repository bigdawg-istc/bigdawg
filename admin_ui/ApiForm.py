"""
Utilities for importing data

See example usage at the bottom.
"""
import json, numbers
import os
import psycopg2
from Util import Util

class ApiForm:
    """
    Handles interaction with the Catalog databaseq
    """
    def __init__(self, catalog_client):
        self.catalog_client = catalog_client

    def processApiForm(self, data):
        data_import = json.loads(data)
        print data
        print data_import
        if not data_import:
            return Util.error_msg("could not parse json")

        if data_import.has_key('api'):
            result = self.processApi(data_import['api'])
            if result != True:
                return result



    def processApi(self, api_data):
        name = api_data['name']
        host = None
        if (api_data.has_key('host')):
            host = api_data['host']

        port = None
        if (api_data.has_key('port')):
            post = api_data['port']

        connection_properties = None
        if (api_data.has_key('connection_properties')):
            connection_properties = api_data['connection_properties']

        result = self.catalog_client.insert_engine(name, host, port, connection_properties)
        if result != True:
            return Util.error_msg(result)

        return

    def processEndpoint(self, endpoint_data):
        name = endpoint_data['name']
