"""
Utilities for importing data

See example usage at the bottom.
"""
import json
from Util import Util

class ApiForm:
    """
    Handles interaction with the Catalog databaseq
    """
    def __init__(self, catalog_client):
        self.catalog_client = catalog_client

    def processApiForm(self, data):
        data_api = json.loads(data)
        if not data_api:
            return Util.error_msg("could not parse json")

        eid = None
        if data_api.has_key('api'):
            if data_api.has_key('endpoint'):
                # Check for duplicate url first, otherwise we'll create the endpoint first and then hit the error
                url = data_api['endpoint']['url']
                if self.catalog_client.get_object_by_name_island_name(url, "API") is not None:
                    return Util.error_msg("Duplicate url for API Island: " + url)

            result = self.processApi(data_api['api'])
            if isinstance(result, int):
                eid = result
            else:
                return Util.error_msg(result)

        if data_api.has_key('endpoint'):
            result = self.processEndpoint(data_api['endpoint'], eid)

            if result != True:
                return Util.error_msg(result)

        return Util.success_msg()

    def processApi(self, api_data):
        name = api_data['name']
        if self.catalog_client.get_engine_by_name(name) is not None:
            return "Duplicate engine: " + name

        island = self.catalog_client.get_island_by_scope_name('API')
        if island is None:
            return "Could not find API Island in catalog"

        islandId = island[0]

        host = None
        if (api_data.has_key('host')):
            host = api_data['host']

        port = None
        if (api_data.has_key('port')):
            port = api_data['port']

        connection_properties = None
        if (api_data.has_key('connection_properties')):
            connection_properties = api_data['connection_properties']

        result = self.catalog_client.insert_engine(name, host, port, connection_properties)
        if not isinstance(result, int):
            return result

        shim_result = self.catalog_client.insert_shim(islandId, result)
        if not isinstance(shim_result, int):
            return shim_result

        return result

    def processEndpoint(self, endpoint_data, engine_id):
        name = endpoint_data['name']
        if engine_id is None:
            engine_id = endpoint_data['engine_id']

        if self.catalog_client.get_engine(engine_id) is None:
            return "Can't find engine: " + engine_id

        if self.catalog_client.get_endpoint_by_engine_id_name(engine_id, name) is not None:
            return "Duplicate endpoint for engine: " + name

        url = endpoint_data['url']
        if self.catalog_client.get_object_by_name_island_name(url, "API") is not None:
            return "Duplicate url for API Island: " + url

        result = self.catalog_client.insert_database(engine_id, name, None, None)
        if isinstance(result, str):
            return result

        dbid = result
        fields = None

        if endpoint_data.has_key('result_key'):
            fields = endpoint_data['result_key']

        result = self.catalog_client.insert_object(url, fields, dbid, dbid)
        if isinstance(result, str):
            return result

        return True