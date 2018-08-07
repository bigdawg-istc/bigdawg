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

    def delete_api_by_dbid(self, dbid):
        database = self.catalog_client.get_database(dbid)
        if database is None:
            return Util.error_msg("database is None for dbid: " + str(dbid))

        engine = self.catalog_client.get_engine(database[1])

        if engine is None:
            return Util.error_msg("engine is None for engine_id: " + str(database[1]))

        objects = self.catalog_client.get_objects_by_phsyical_db(dbid)

        if objects is None:
            return Util.error_msg("objects is None for phsyical_db: " + str(dbid))
        elif len(objects) == 0:
            return Util.error_msg("len(objects) is 0 for phsyical_db: " + str(dbid))

        island = self.catalog_client.get_island_by_scope_name('API')
        if island is None:
            return Util.error_msg("island is none for scope_name API")

        deleteEngine = False
        databases = self.catalog_client.get_databases_by_engine_id(engine[0])
        if databases is None or len(databases) == 0:
            return Util.error_msg("databases not found for engine: " + str(engine[0]))

        if len(databases) == 1:
            deleteEngine = True

        object = objects[0]
        response = self.catalog_client.delete_object(object[0])
        if response != True:
            return Util.error_msg(response)

        response = self.catalog_client.delete_database(dbid)
        if response != True:
            return Util.error_msg(response)

        if deleteEngine:
            response = self.catalog_client.delete_shim(island[0], engine[0])
            if response != True:
                return Util.error_msg(response)

            response = self.catalog_client.delete_engine(engine[0])
            if response != True:
                return Util.error_msg(response)

        return Util.success_msg()

    def process_api_form(self, data):
        dataApi = json.loads(data)
        if not dataApi:
            return Util.error_msg("could not parse json")

        eid = None
        if dataApi.has_key('api'):
            if dataApi.has_key('endpoint'):
                endpoint_data = dataApi['endpoint']
                # Check for duplicate url first, otherwise we'll create the API first and then hit the error
                url = endpoint_data['url']
                oid = None
                if endpoint_data.has_key('oid'):
                    oid = endpoint_data['oid']

                testObject = self.catalog_client.get_object_by_name_island_name(url, "API")
                if oid is not None:
                    if testObject is None:
                        testObject = self.catalog_client.get_object(oid)
                        if testObject is None:
                            return Util.error_msg("Unknown object id: " + oid)
                    if int(testObject[0]) != int(oid):
                        return Util.error_msg("Duplicate url for API Island: " + url)
                elif testObject is not None:
                    return Util.error_msg("Duplicate url for API Island: " + url)

            result = self.process_api(dataApi['api'])
            if isinstance(result, int):
                eid = result
            else:
                return Util.error_msg(result)

        if dataApi.has_key('endpoint'):
            result = self.process_endpoint(dataApi['endpoint'], eid)

            if result != True:
                return Util.error_msg(result)

        return Util.success_msg()

    def process_api(self, apiData):
        name = apiData['name']
        eid = None
        if apiData.has_key('eid'):
            eid = apiData['eid']

        testEngine = self.catalog_client.get_engine_by_name(name)

        if eid is not None:
            if testEngine is None:
                testEngine = self.catalog_client.get_engine(eid)
                if testEngine is None:
                    return "Unknown engine: " + eid
            if int(eid) != int(testEngine[0]):
                return "Duplicate engine: " + name
        elif testEngine is not None:
            return "Duplicate engine: " + name

        island = self.catalog_client.get_island_by_scope_name('API')
        if island is None:
            return "Could not find API Island in catalog"

        islandId = island[0]

        host = None
        if (apiData.has_key('host')):
            host = apiData['host']

        port = None
        if (apiData.has_key('port')):
            port = apiData['port']

        connection_properties = None
        if (apiData.has_key('connection_properties')):
            connection_properties = apiData['connection_properties']

        # update case
        if eid is not None:
            result = self.catalog_client.update_engine(eid, name, host, port, connection_properties)
            if result != True:
                return result

            return int(eid)

        # insert case
        result = self.catalog_client.insert_engine(name, host, port, connection_properties)
        if not isinstance(result, int):
            return result

        shim_result = self.catalog_client.insert_shim(islandId, result)
        if not isinstance(shim_result, int):
            return shim_result

        return result

    def process_endpoint(self, endpointData, engineId):
        name = endpointData['name']
        if engineId is None:
            engineId = endpointData['engine_id']

        if self.catalog_client.get_engine(engineId) is None:
            return "Can't find engine: " + engineId

        dbid = None
        if endpointData.has_key('dbid'):
            dbid = endpointData['dbid']

        testDatabase = self.catalog_client.get_database_by_engine_id_name(engineId, name)
        if dbid is not None:
            if testDatabase is None:
                testDatabase = self.catalog_client.get_database(dbid)
                if testDatabase is None:
                    return "Unknown database: " + dbid
            if int(dbid) != int(testDatabase[0]):
                return "Duplicate endpoint for engine: " + name
        elif testDatabase is not None:
            return "Duplicate endpoint for engine: " + name

        url = endpointData['url']
        oid = None
        if endpointData.has_key('oid'):
            oid = endpointData['oid']

        testObject = self.catalog_client.get_object_by_name_island_name(url, "API")
        if oid is not None:
            if testObject is None:
                testObject = self.catalog_client.get_object(oid)
                if testObject is None:
                    return "Unknown object id: " + oid
            if int(testObject[0]) != int(oid):
                return "Duplicate url for API Island: " + url
        elif testObject is not None:
            return Util.error_msg("Duplicate url for API Island: " + url)

        password_field = None
        if endpointData.has_key("password_field"):
            password_field = endpointData['password_field']

        if dbid is not None:
            result = self.catalog_client.update_database(dbid, engineId, name, None, password_field)
            if result != True:
                return result
        else:
            result = self.catalog_client.insert_database(engineId, name, None, password_field)
            if isinstance(result, str):
                return result
            dbid = result

        fields = None
        if endpointData.has_key('result_key'):
            fields = endpointData['result_key']

        if oid is not None:
            result = self.catalog_client.update_object(oid, url, fields, dbid, dbid)
            if result != True:
                return result
        else:
            result = self.catalog_client.insert_object(url, fields, dbid, dbid)
            if isinstance(result, str):
                return result

        return True