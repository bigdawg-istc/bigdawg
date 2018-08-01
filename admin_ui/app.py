# To run:
#    First:
#       cp env.sample .env
#       # Make any necessary adjustments to .env
#
#    Then:
#       export FLASK_APP=app.py
#       flask run --host=0.0.0.0

from flask import Flask, redirect, render_template, render_template_string, request, url_for
from dotenv import load_dotenv
from os.path import join, dirname
from DockerClient import DockerClient
from CatalogClient import CatalogClient
from SchemaClient import SchemaClient
from QueryClient import QueryClient
from Importer import Importer
from ApiForm import ApiForm
from Util import Util
import os, json
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

app = Flask(__name__)
def read_catalog_credentials():
    catalog_cred = { "database": os.environ.get('CATALOG_DATABASE'),
                     "user": os.environ.get('CATALOG_USER'),
                     "password": os.environ.get('CATALOG_PASSWORD'),
                     "host": os.environ.get('CATALOG_HOST'),
                     "port": os.environ.get('CATALOG_PORT')
                     }
    return catalog_cred

def read_schema_credentials():
    schema_cred = { "database": os.environ.get('SCHEMA_DATABASE'),
                     "user": os.environ.get('SCHEMA_USER') or os.environ.get('CATALOG_USER'),
                     "password": os.environ.get('SCHEMA_PASSWORD') or os.environ.get('CATALOG_PASSWORD'),
                     "host": os.environ.get('SCHEMA_HOST') or os.environ.get('CATALOG_HOST'),
                     "port": os.environ.get('SCHEMA_PORT') or os.environ.get('CATALOG_PORT')
                     }
    return schema_cred

# Get catalog credentials for the CatalogClient
catalog_cred = read_catalog_credentials()
schema_cred = read_schema_credentials()
versions = {
    "util.js": os.stat('static/js/util.js').st_mtime,
    "query.js": os.stat('static/js/query.js').st_mtime,
    "api.js": os.stat('static/js/api.js').st_mtime,
    "import.js": os.stat('static/js/import.js').st_mtime,
    "general.css": os.stat('static/css/general.css').st_mtime
}
def getCatalogClient():
    return CatalogClient(
        database=catalog_cred['database'],
        user=catalog_cred['user'],
        password=catalog_cred['password'],
        host=catalog_cred['host'],
        port=catalog_cred['port'])

def getCatalogData():
    catalog_client = getCatalogClient()
    objects = catalog_client.get_objects()
    engines = catalog_client.get_engines()
    databases = catalog_client.get_databases()
    enginesByEngineId = {}
    for engine in engines:
        enginesByEngineId[engine[0]] = engine

    return {'objects': objects, 'engines': engines, 'databases': databases, 'engines_by_engine_id': enginesByEngineId}

def getSchemaClient():
    return SchemaClient(
        database=schema_cred['database'],
        user=schema_cred['user'],
        password=schema_cred['password'],
        host=schema_cred['host'],
        port=schema_cred['port'])

def getSchemaData():
    schema_client = getSchemaClient()
    datatypes = schema_client.get_datatypes()
    return {'datatypes': datatypes}

# Cluster Status
@app.route('/')
def status():
    containers = DockerClient().list_containers()
    return render_template('status.html', containers=containers)


# Data Catalog
@app.route('/catalog')
def catalog():
    catalog_data = getCatalogData()
    objects = catalog_data['objects']
    engines = catalog_data['engines']
    # Template expects results like:
    # objects = [
    #     (0, 'mimic2v26.a_chartdurations', 'subject_id,icustay_id,itemid,elemid,starttime,startrealtime,endtime,cuid,duration', 2, 3), 
    #     (1, 'mimic2v26.a_iodurations', 'subject_id,icustay_id,itemid,elemid,starttime,startrealtime,endtime,cuid,duration', 2, 3), 
    #     (2, 'mimic2v26.a_meddurations', 'subject_id,icustay_id,itemid,elemid,starttime,startrealtime,endtime,cuid,duration', 2, 3), 
    #     (3, 'mimic2v26.additives', 'subject_id,icustay_id,itemid,ioitemid,charttime,elemid,cgid,cuid,amount,doseunits,route', 2, 3), 
    #     (4, 'mimic2v26.admissions', 'hadm_id,subject_id,admit_dt,disch_dt', 2, 2), 
    #     (5, 'mimic2v26.censusevents', 'census_id,subject_id,intime,outtime,careunit,destcareunit,dischstatus,los,icustay_id', 2, 2), 
    #     (6, 'mimic2v26.chartevents', 'subject_id,icustay_id,itemid,charttime,elemid,realtime,cgid,cuid,value1,value1num,value1uom,value2,value2num,value2uom,resultstatus,stopped', 2, 3), 
    #     (7, 'mimic2v26.comorbidity_scores', 'subject_id,hadm_id,category,congestive_heart_failure,cardiac_arrhythmias,valvular_disease,pulmonary_circulation,peripheral_vascular,hypertension,paralysis,other_neurological,chronic_pulmonary,diabetes_uncomplicated,diabetes_complicated,hypothyroidism,renal_failure,liver_disease,peptic_ulcer,aids,lymphoma,metastatic_cancer,solid_tumor,rheumatoid_arthritis,coagulopathy,obesity,weight_loss,fluid_electrolyte,blood_loss_anemia,deficiency_anemias,alcohol_abuse,drug_abuse,psychoses,depression', 2, 3), 
    #     (8, 'mimic2v26.d_caregivers', 'cgid,label', 2, 2), 
    #     (9, 'mimic2v26.d_careunits', 'cuid,label', 2, 2), 
    #     (10, 'mimic2v26.d_chartitems', 'itemid,label,category,description', 2, 2), 
    # ]
    return render_template('catalog.html', objects=objects, engines=engines)

@app.route('/import')
def import_page():
    catalog_client = getCatalogClient()
    engines = catalog_client.get_engines_excluding_island('API')
    databases = catalog_client.get_databases_excluding_island('API')
    objects = catalog_client.get_objects()
    enginesByEngineId = {}
    for engine in engines:
        enginesByEngineId[engine[0]] = engine

    schema_data = getSchemaData()
    return render_template('import.html',
                           versions=versions,
                           enginesByEngineId=enginesByEngineId,
                           objects=objects,
                           databases=databases,
                           datatypes=schema_data['datatypes'])

@app.route('/import_csv', methods=["POST"])
def import_csv():
    importer = Importer(getCatalogClient(), getSchemaClient())
    result = importer.import_data(request.data)
    return render_template_string(result)

@app.route('/get_schemas', methods=["POST"])
def get_schemas():
    importer = Importer(getCatalogClient(), getSchemaClient())
    result = importer.get_schemas(request.data)
    return render_template_string(result)

@app.route('/query')
def query():
    return render_template('query.html', versions=versions)

@app.route('/api', methods=["GET"])
def api():
    catalog_client = getCatalogClient()
    engines = catalog_client.get_engines_by_island('API')
    databases = catalog_client.get_databases_by_island('API')
    enginesByEngineId = {}
    for engine in engines:
        enginesByEngineId[engine[0]] = engine

    return render_template('api.html',
                           versions=versions,
                           engines=engines,
                           databases=databases,
                           enginesByEngineID=enginesByEngineId)

@app.route('/api_list', methods=["GET"])
def api_list():
    catalog_client = getCatalogClient()
    engines = catalog_client.get_engines_by_island('API')
    databases = catalog_client.get_databases_by_island('API')
    return render_template_string(json.JSONEncoder().encode({"success": True, "engines": engines, "databases": databases}))

@app.route('/api/<int:dbid>', methods=["DELETE"])
def api_delete(dbid):
    api_form = ApiForm(getCatalogClient())
    result = api_form.delete_api_by_dbid(dbid)
    return render_template_string(result)

@app.route('/api/<int:dbid>', methods=["GET"])
def api_show(dbid):
    catalog_client = getCatalogClient()
    database = catalog_client.get_database(dbid)
    engine = catalog_client.get_engine(database[1])
    objects = catalog_client.get_objects_by_phsyical_db(dbid)
    object = objects[0]
    return render_template_string(json.JSONEncoder().encode({"success": True, "engine": engine, "database": database, "object": object}))

@app.route('/api_form', methods=["POST"])
def api_form_post():
    api_form = ApiForm(getCatalogClient())
    result = api_form.process_api_form(request.data)
    return render_template_string(result)

@app.route('/get_engine_by_name', methods=["POST"])
def get_engine_by_name():
    requestObj = json.loads(request.data)
    catalogClient = getCatalogClient()
    engine = catalogClient.get_engine_by_name(requestObj['name'])
    if engine is None:
        return render_template_string(Util.error_msg("not found"))
    return render_template_string(json.JSONEncoder().encode({"success": True, "engine": engine}))

@app.route('/run_query', methods=["POST"])
def runQuery():
    query = request.data
    print os.environ.get('QUERY_SCHEME'),os.environ.get('QUERY_HOST'),int(os.environ.get('QUERY_PORT'))
    result = QueryClient(os.environ.get('QUERY_SCHEME'),os.environ.get('QUERY_HOST'),int(os.environ.get('QUERY_PORT'))).run_query(query)
    return render_template_string(result)

# Important Links
@app.route('/links')
def links():
    links = [
        {
            "name": "Documentation",
            "href": "http://bigdawg-documentation.readthedocs.io/en/latest/index.html"
        },
        {
            "name": "Code",
            "href": "https://bitbucket.org/aelmore/bigdawgmiddle"
        },
        {
            "name": "Installation and Setup",
            "href": "http://bigdawg-documentation.readthedocs.io/en/latest/getting-started.html"
        },
    ]
    return render_template('links.html', links=links)

# Start container
@app.route('/startContainer', methods=["POST"])
def startContainer():
    action = request.form['action']
    container_name = request.form['container']
    DockerClient().start_container(container_name)
    return redirect(url_for('status'))

# Stop container
@app.route('/stopContainer', methods=["POST"])
def stopContainer():
    action = request.form['action']
    container_name = request.form['container']
    DockerClient().stop_container(container_name)
    return redirect(url_for('status'))

if __name__ == '__main__':
    # Use this line to run the server visible to external connections.
    # app.run(host='0.0.0.0')
    app.run(threaded=True)
