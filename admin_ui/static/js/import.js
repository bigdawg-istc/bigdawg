class ImportCSV extends Evented {
    constructor() {
        super();
        this.fileEle = document.querySelector('input[type=file]');
        this.uploadCSVEle = document.getElementById('upload-csv');
        this.csvErrorEle = document.getElementById('csv-error');
        this.validateCSVEle = document.getElementById('validate-csv');
        this.resetCSVEle = document.getElementById('reset-csv');
        this.formEle = document.querySelector('form.upload-form');
        this.loadingEle = document.getElementById('upload-loading');
        this.uploadErrorEle = document.getElementById('upload-error');
        this.filenameEle = document.querySelector('label[for=file-input]');
        this.headerEle = document.querySelector('input[type=checkbox]');
        this.csvShow = document.getElementById('csv-show');
        this.csvText = document.getElementById('csv-text');
        this.csvButtons = document.getElementById('csv-buttons');
        this.currentCSV = null;
        this.oid = null;
        this.csvTextOrigRows = parseInt(this.csvText.rows);
        this.schema = new Schema();
        this.database = null;
        this.engine = null;
        this.hide();
        this.addEventListeners();
    }

    setName(name) {
        this.schema.setName(name);
        this.name = name;
    }

    setSchemaName(schemaName) {
        this.schema.setSchemaName(schemaName);
        this.schemaName = schemaName;
    }

    getEngineType() {
        return this.schema.getEngineType();
    }

    show() {
        this.formEle.classList.remove('hidden');
        this.hidden = false;
    }

    hide() {
        this.formEle.classList.add('hidden');
        this.schema.hide();
        this.hidden = true;
    }

    isHidden() {
        return this.hidden;
    }

    setOid(oid) {
        this.oid = oid;
    }

    setEngine(engine) {
        this.engine = engine;
        this.schema.setEngine(engine);
    }

    setDatabase(database) {
        this.database = database;
        this.schema.setDatabase(database);
    }

    addEventListeners() {
        this.formEle.addEventListener('submit', (e) => { e.preventDefault(); return false; });
        this.fileEle.addEventListener('change', this.checkFile.bind(this));
        this.uploadCSVEle.addEventListener('click', this.uploadCsv.bind(this));
        this.headerEle.addEventListener('change', this.verifyHeader.bind(this));
        this.resetCSVEle.addEventListener('click', this.partialReset.bind(this));
        this.validateCSVEle.addEventListener('click', this.updateCSV.bind(this));
        this.csvText.addEventListener('keyup', this.editCSV.bind(this));
        this.csvText.addEventListener('change', this.editCSV.bind(this));
        this.schema.addEventListener('input-change', this.checkSchema.bind(this));
        this.schema.addEventListener('regenerate-create-table', this.checkSchema.bind(this));
        this.schema.addEventListener('regenerate-schema', this.checkSchema.bind(this));
        this.schema.addEventListener('create-syntax-change', this.checkSchema.bind(this));
        this.schema.addEventListener('schema-syntax-change', this.checkSchema.bind(this));
    }

    checkSchema() {
        if (!this.schema.isHidden() && this.schema.isReady()) {
            this.uploadCSVEle.classList.remove('hidden');
            return;
        }
        this.uploadCSVEle.classList.add('hidden');
    }

    startLoading() {
        this.uploadCSVEle.disabled = true;
        this.loadingEle.style.visibility = 'visible';
        document.querySelectorAll('input').forEach(item => item.disabled = true);
        document.querySelectorAll('select').forEach(item => item.disabled = true);
        document.querySelectorAll('textarea').forEach(item => item.disabled = true);
    }

    stopLoading() {
        this.uploadCSVEle.disabled = false;
        this.loadingEle.style.visibility = 'hidden';
        document.querySelectorAll('input').forEach(item => item.disabled = false);
        document.querySelectorAll('select').forEach(item => item.disabled = false);
        document.querySelectorAll('textarea').forEach(item => item.disabled = false);
    }

    showError(msg) {
        this.uploadErrorEle.innerText = msg;
        this.uploadErrorEle.classList.remove('hidden');
    }

    hideError() {
        this.uploadErrorEle.classList.add('hidden');
    }

    showSuccess(msg) {
        this.fireEvent('success');
    }

    uploadCsv() {
        this.hideError();
        this.startLoading();
        const csv = this.currentCSV;
        const oid = this.oid;
        if (null === oid) {
            if (this.schema.isHidden()) {
                this.stopLoading();
                this.showError('No object selected.');
                return;
            }
            if (!this.schema.isReady()) {
                this.stopLoading();
                this.showError('Incomplete schema.');
                return;
            }
        }

        if (csv === null) {
            this.stopLoading();
            let msg = 'CSV not yet uploaded';
            if (this.fileEle.files && this.fileEle.files[0]) {
                msg = 'CSV not in proper format, please fix first';
            }
            this.showError(msg);
            return;
        }

        const csvData = csv.get();
        if (this.headerEle.checked) {
            csvData.shift();
        }

        const uploadData = { csv: csvData };


        if (oid !== null) {
            uploadData.oid = oid;
        }
        else {
            uploadData.dbid = this.database.dbid;
            uploadData.create = this.schema.getCreateTable();
            uploadData.schema = this.schema.getCreateSchema();
            uploadData.fields = this.schema.getFieldsList();
            uploadData.fields_name = this.name;
            if (this.getEngineType() === 'postgres' || this.getEngineType() === 'vertica') {
                if (this.schemaName) {
                    uploadData.fields_name = this.schemaName + '.' + this.name;
                }
            }
            uploadData.name = this.name;
        }

        const upload = JSON.stringify(uploadData);
        this.doUpload(upload);
    }

    doUpload(val) {
        fetchJson('/import_csv', val).then(() => {
            this.stopLoading();
            this.showSuccess('Success');
        }, (rejectResult) => {
           this.stopLoading();
           this.showError(rejectResult);
        });
    }

    setFilename(name) {
        this.filenameEle.innerText = name;
    }

    resetFilename() {
        this.filenameEle.innerText = 'Choose file';
    }

    checkFile() {
        if (!this.fileEle.files || !this.fileEle.files[0]) {
            return;
        }

        const file = this.fileEle.files[0];
        if (file.name) {
            this.setFilename(file.name);
        }
        console.log(file, file.name);
        this.readFile(file);
    }

    partialReset() {
        this.csvShow.classList.add('hidden');
        this.csvErrorEle.classList.add('hidden');
        this.csvErrorEle.innerText = '';
        this.csvText.classList.remove('csv-error');
        this.csvText.classList.remove('csv-ok');
        this.fileEle.value = null;
        this.headerEle.checked = false;
        this.resetFilename();
        this.schema.reset();
        this.schema.hide();
        this.hideError();
    }

    reset() {
        this.partialReset();
        this.uploadCSVEle.classList.add('hidden');
        this.oid = null;
    }

    readFile(file) {
        const reader = new FileReader();
        reader.addEventListener('load', this.processCSVUpload.bind(this));
        reader.readAsText(file);
    }

    displayCSV(csv) {
        const csvLines = csv.getLines();
        this.csvText.value = csvLines.join("\n");
        const csvLinesLength = csvLines.length > 5 ? csvLines.length : csvLines.length + 1;
        this.csvText.rows = csvLines.length > this.csvTextOrigRows ? this.csvTextOrigRows : csvLinesLength;
        this.csvShow.classList.remove('hidden');
        this.csvButtons.classList.add('hidden');
    }

    editCSV(csv) {
        if (this.csvText.value === this.csvTextLastValue) {
            return;
        }
        if (!this.uploadCSVEle.classList.contains('hidden')) {
            this.uploadCSVEle.classList.add('hidden');
        }
        if (this.csvButtons.classList.contains('hidden')) {
            this.csvButtons.classList.remove('hidden');
        }
        if (!this.uploadSuccessEle.classList.contains('hidden')) {
            this.uploadSuccessEle.classList.add('hidden');
        }
        this.csvTextLastValue = this.csvText.value;
    }

    updateCSV() {
        this.processCSVData(this.csvText.value);
    }

    processCSVUpload(e) {
        const data = e.target.result;
        this.processCSVData(data);
    }

    processCSVData(data) {
        this.hideError();
        this.currentCSV = null;
        this.uploadCSVEle.classList.add('hidden');
        this.csvButtons.classList.add('hidden');

        if (this.oid === null) {
            const csv = new CSV();
            csv.parse(data);
            this.displayCSV(csv);
            this.currentCSV = csv;
            this.schema.setEngine(this.engine);
            this.schema.setDatabase(this.database);
            this.schema.setName(this.name);
            this.schema.setSchemaName(this.schemaName);
            this.schema.setCSV(this.currentCSV.get(), this.headerEle.checked);
            if (this.schema.isHidden()) {
                this.schema.show();
            }
            this.checkSchema();

            return;
        }
        const objectSelected = this.oid;

        if (null === objectSelected) {
            this.showError('Error: no selected object');
            this.fileEle.value = null;
            return;
        }
        const object = objectsById[objectSelected];
        if (!object) {
            this.showError('No object found for oid: ' + objectSelected);
            this.fileEle.value = null;
            return;
        }
        const objectInfo = this.getObjectInfo();
        if (!objectInfo) {
            return;
        }
        const { fields, fieldsSplit, fieldsCount } = objectInfo;
        const csv = new CSV();
        csv.parse(data);
        this.displayCSV(csv);
        const csvFinal = [];
        const errors = [];

        csv.get().forEach((line, idx) => {
            if (line.length !== fieldsCount) {
                errors.push('Line ' + (idx + 1) + ' has wrong number of fields: ' + line.length + ' while expecting ' + fieldsCount);
                return;
            }
            if (idx === 0) {
                // Check if header line
                let header = true;
                line.forEach((member, idx) => {
                    if (fieldsSplit[idx] !== member) {
                        console.log('header', header);
                        header = false;
                    }
                });
                if (header) {
                    this.headerEle.checked = true;
                    csvFinal.push(line);
                    return;
                }
            }

            // convert types
            fieldsSplit.forEach((item, idx) => {
                if (datatypesByTable[object.name] &&
                        datatypesByTable[object.name][fieldsSplit[idx]]) {
                    const dataType = datatypesByTable[object.name][fieldsSplit[idx]];
                    if (dataType === 'integer') {
                        line[idx] = parseInt(line[idx]);
                    }
                    if (dataType === 'double precision') {
                        line[idx] = parseFloat(line[idx]);
                    }
                }
            });

            csvFinal.push(line);
        });
        if (errors.length) {
            this.csvButtons.classList.remove('hidden');
            this.csvText.classList.add('csv-error');
            this.csvErrorEle.classList.remove('hidden');
            this.csvErrorEle.innerText = errors.join("\n");
            return;
        }
        this.currentCSV = new CSV();
        this.currentCSV.set(csvFinal);
        this.csvErrorEle.classList.add('hidden');
        this.csvText.classList.add('csv-ok');
        this.hideError();
        this.uploadCSVEle.classList.remove('hidden');
    }

    getObjectInfo() {
        if (this.oid === null) {
            this.showError('Error: no selected object');
            return;
        }

        const object = objectsById[this.oid];
        if (!object) {
            this.showError('No object found for oid: ' + this.oid);
            return;
        }
        const fields = object.fields;
        const fieldsSplit = fields.split(',');
        const fieldsCount = fieldsSplit.length;
        return { fields, fieldsSplit, fieldsCount };
    }

    verifyHeader() {
        if (!this.headerEle.checked) {
            return;
        }

        if (null === this.currentCSV) {
            return;
        }

        if (this.currentCSV.get().length === 0) {
            this.showError('CSV is empty');
            return;
        }
        if (!this.oid) {
            this.schema.setCSV(this.currentCSV.get(), true);
            return;
        }

        const objectInfo = this.getObjectInfo();
        const { fields, fieldsSplit, fieldsCount } = objectInfo;
        const csvLine = this.currentCSV.get()[0];

        let header = true;
        csvLine.forEach((member, idx) => {
            if (fieldsSplit[idx] !== member) {
                header = false;
            }
        });

        if (!header) {
            this.headerEle.checked = false;
            this.showError("CSV headings don't match with the list of fields above");
            return;
        }

        return true;
    }
}

class DesiredFormat {
    constructor() {
        this.desiredFormatEle = document.getElementById('desired-format');
        this.desiredFormatWrapEle = document.getElementById('desired-format-wrap');
    }

    reset() {
        this.desiredFormatEle.innerHTML = '';
        this.hide();
    }

    hide() {
        this.desiredFormatWrapEle.classList.add('hidden');
    }

    show() {
        this.desiredFormatWrapEle.classList.remove('hidden');
    }


    set(oid) {
        if (objectsById[oid]) {
            this.desiredFormatEle.innerHTML = escapeSpecial(objectsById[oid].fields);
            this.show();
            return true;
        }
    }


}

class Chooser {
    constructor() {
        this.desiredFormat = new DesiredFormat();
        this.databaseEle = document.getElementById('database');
        this.objectEle = document.getElementById('object');
        this.newDiv = document.querySelector('div.new');
        this.existingDiv = document.querySelector('div.existing');
        this.formEle = document.querySelector('form.selector');
        this.newEle = document.getElementById('new');
        this.schemaNameEle = document.getElementById('schema-name');
        this.schemaNameLabel = document.querySelector('label[for=schema-name]');
        this.importCSV = new ImportCSV();
        this.objectSelection = document.querySelector('.object-selection');
        this.schemaLoadingEle = document.getElementById('schema-loading');
        this.databaseEle.selectedIndex = 0;
        this.successEle = document.getElementById('success');
        this.part1Ele = document.getElementById('part-1');
        this.uploadAnotherEle = document.getElementById('upload-another');
        this.addEventListeners();
    }

    success() {
        this.successEle.innerText = 'Success';
        this.successEle.classList.remove('hidden');
        this.importCSV.hide();
        this.desiredFormat.hide();
        this.uploadAnotherEle.classList.remove('hidden');
        this.hide();
    }

    uploadAnother() {
        window.location.reload();
    }

    hide() {
        this.part1Ele.classList.add('hidden');
    }

    addEventListeners() {
        this.uploadAnotherEle.addEventListener('click', this.uploadAnother.bind(this));
        this.databaseEle.addEventListener('change', this.populateObjects.bind(this));
        this.objectEle.addEventListener('change', this.setDesiredFormat.bind(this));
        this.formEle.addEventListener('submit', Chooser.submit);
        this.newEle.addEventListener('change', this.onNew.bind(this));
        this.newEle.addEventListener('keyup', this.onNew.bind(this));
        this.schemaNameEle.addEventListener('change', this.onSchemaName.bind(this));
        document.querySelectorAll('input[name=type]').forEach((element) => {
           element.addEventListener('change', this.toggleObjectInput.bind(this));
        });
        this.importCSV.addEventListener('success', this.success.bind(this));
    }

    static submit(e) {
        e.stopPropagation();
        e.preventDefault();
        return false;
    }

    fillSchemaName(dbid) {
        if (this.importCSV.getEngineType() !== 'postgres' && this.importCSV.getEngineType() !== 'vertica') {
            return;
        }

        this.lastDbid = dbid;
        this.schemaLoading = true;
        this.schemaLoadingEle.style.display='block';
        this.schemaNameEle.classList.add('hidden');
        this.schemaNameLabel.classList.add('hidden');
        fetchJson('/get_schemas', JSON.stringify({'dbid': dbid})).then((result) => {
            if (!result.schemas) {
                console.log("No schemas in /get_schemas result for " + dbid, result);
                return;
            }

            if (!dbid === this.lastDbid) { // in case multiple of these get fired off
                return;
            }

            if (this.schemaNameEle.selectedIndex >= 0) {
                this.schemaNameEle.selectedIndex = -1;
            }

            const childNodes = this.schemaNameEle.childNodes;
            const childNodesArr = Array.from(childNodes);
            childNodesArr.forEach(node => this.schemaNameEle.removeChild(node));

            result.schemas.forEach((schema) => {
                const optionEle = document.createElement('option');
                optionEle.value = schema;
                optionEle.innerText = schema;
                this.schemaNameEle.appendChild(optionEle);
            });

            this.schemaLoadingEle.style.display='none';
            this.schemaNameEle.classList.remove('hidden');
            this.schemaNameLabel.classList.remove('hidden');
            this.schemaLoading = false;
        });
    }

    setDesiredFormat() {
        this.desiredFormat.reset();
        if (!this.objectEle.selectedOptions) {
            return;
        }

        if (this.state === 'new') {
            return;
        }

        const selectedOption = this.objectEle.selectedOptions[0];
        if (selectedOption.value === '' || selectedOption.value === null) {
            return;
        }
        const oid = parseInt(selectedOption.value);
        if (this.desiredFormat.set(oid)) {
            this.importCSV.setOid(oid);
            this.importCSV.show();
            this.newEle.value = "";
        }
    }

    reset() {
        this.schemaNameEle.value = null;
        this.newEle.value = null;
        this.importCSV.reset();
    }

    toggleSchemaName() {
        if (this.schemaLoading) {
            return;
        }
        if (this.importCSV.getEngineType() === 'postgres' || this.importCSV.getEngineType() === 'vertica') {
            this.schemaNameEle.classList.remove('hidden');
            this.schemaNameLabel.classList.remove('hidden');
            return;
        }

        this.schemaNameEle.classList.add('hidden');
        this.schemaNameLabel.classList.add('hidden');
    }

    toggleObjectInput() {
        document.querySelectorAll('input[name=type]').forEach((element) => {
            if (!element.checked) {
                return;
            }
            this.importCSV.reset();
            switch (element.value) {
                case 'new':
                    if (this.state !== 'new') {
                        this.state = 'new';
                        this.hideExisting();
                        this.importCSV.reset();
                        this.importCSV.hide();
                        const { _, database, engine } = this.getSelectedInfo();
                        this.importCSV.setDatabase(database);
                        this.importCSV.setEngine(engine);
                        this.newDiv.classList.remove('hidden');
                        this.toggleSchemaName();
                    }
                    break;
                case 'existing':
                    if (this.state !== 'existing') {
                        this.state = 'existing';
                        this.newDiv.classList.add('hidden');
                        this.importCSV.reset();
                        this.importCSV.hide();
                        this.showExisting();
                    }
                    break;
                default:
                    throw 'Unknown value: ' + element.value;
            }
        });
    }

    onNew(e) {
        if (this.newDiv.classList.contains('hidden')) {
            return;
        }

        if (e.target.value !== "" && e.target.value !== null) {
            this.importCSV.setName(e.target.value);
            if (!this.schemaNameEle.classList.contains('hidden') && this.schemaNameEle.selectedOptions.length === 0) {
                return;
            }

            if (this.importCSV.isHidden()) {
                this.importCSV.show();
            }
        }
    }

    onSchemaName(e) {
        if (this.newDiv.classList.contains('hidden') || this.schemaNameEle.classList.contains('hidden')) {
            return;
        }

        this.importCSV.setSchemaName(e.target.value);
    }

    hideExisting() {
        this.existingDiv.classList.add('hidden');
        this.desiredFormat.reset();
    }

    showExisting() {
        this.existingDiv.classList.remove('hidden');
        this.setDesiredFormat();
    }

    getSelectedInfo() {
        const selectedDatabase = this.databaseEle.selectedOptions[0].value;
        const selectedDatabaseNum = parseInt(selectedDatabase);
        const database = databasesById[selectedDatabase];
        const eid = database['engine_id'];
        const engine = enginesById[eid];
        if (this.objectSelection.classList.contains('hidden')) {
            this.objectSelection.classList.remove('hidden');
        }
        return { selectedDatabaseNum: selectedDatabaseNum, database: database, engine: engine }
    }

    populateObjects() {
        this.importCSV.reset();
        const defaultHtml = '<option>Select Database First</option>';
        if (!this.databaseEle.selectedOptions || this.databaseEle.selectedOptions[0].value === '') {
            this.objectEle.innerHTML = defaultHtml;
            this.desiredFormat.reset();
            if (!this.objectSelection.classList.contains('hidden')) {
                this.objectSelection.classList.add('hidden');
            }
            if (!this.importCSV.isHidden()) {
                this.importCSV.hide();
            }
            return;
        }

        const { selectedDatabaseNum, database, engine } = this.getSelectedInfo();
        this.importCSV.setDatabase(database);
        this.importCSV.setEngine(engine);
        this.fillSchemaName(selectedDatabaseNum);
        this.toggleSchemaName();

        let optionHtml = '';
        if (this.objectSelection.classList.contains('hidden')) {
            this.objectSelection.classList.remove('hidden');
        }
        objects.forEach((item) => {
            if (item.physical_db === selectedDatabaseNum) {
                optionHtml += '<option value="' + item.oid + '">' + escapeSpecial(item.name) + '</option>\n';
            }
        });
        this.objectEle.innerHTML = optionHtml || defaultHtml;
        this.setDesiredFormat();
    }
}

class Schema extends Evented {
    constructor() {
        super();
        this.template = document.querySelector('#csv-schema-wrap template');
        this.fieldsDiv = document.getElementById('csv-fields');
        this.schemaWrapDiv = document.getElementById('csv-schema-wrap');
        this.csvCreateSchemaWrapEle = document.getElementById('csv-create-schema-wrap');
        this.createSyntax = document.getElementById('create-syntax');
        this.schemaSyntax = document.getElementById('schema-syntax');
        this.createSchemaDiv = document.getElementById('create-schema');
        this.regenerateButtonCreate = document.querySelector('#create-table button');
        this.regenerateButtonSchema = document.querySelector('#create-schema button');
        this.setDefaultTypes();
        this.addEventListeners();
        this.hide();
        this.reset();
    }

    addEventListeners() {
        this.createSyntax.addEventListener('change', this.createSyntaxChange.bind(this));
        this.createSyntax.addEventListener('keyup', this.createSyntaxChange.bind(this));
        this.schemaSyntax.addEventListener('change', this.schemaSyntaxChange.bind(this));
        this.schemaSyntax.addEventListener('keyup', this.schemaSyntaxChange.bind(this));
        this.regenerateButtonCreate.addEventListener('click', this.regenerateCreate.bind(this));
        this.regenerateButtonSchema.addEventListener('click', this.regenerateSchema.bind(this));
    }

    getCreateSchema() {
        return this.schemaSyntax.value;
    }

    getCreateTable() {
        return this.createSyntax.value;
    }

    isReady() {
        if (this.showSchema) {
            if (this.generatedCreateTable !== this.createSyntax.value && this.generatedSchema !== this.schemaSyntax.value) {
                return true;
            }
        }

        const columnNames = this.getListFromColumns('name');
        let empty = false;
        columnNames.forEach((name) => {
            if (name === null || name === '') {
                empty = true;
            }
        });

        if (empty) {
            return false;
        }

        if (!this.showTypes) {
            return true;
        }

        const columnTypes = this.getTypesList('type');

        columnTypes.forEach((type) => {
            if (type === null || type === '') {
                empty = true;
            }
        });

        return !empty;
    }

    createSyntaxChange() {
        if (this.generatedCreateTable !== this.createSyntax.value) {
            this.regenerateButtonCreate.classList.remove('hidden');
        }
        else {
            // unhide /disable types
            this.regenerateButtonCreate.classList.add('hidden');
        }
        if (this.savedCreateTable !== this.createSyntax.value) {
            this.fireEvent('create-syntax-change');
            if (this.getEngineType() === 'postgres') {
                this.savedSchema = this.createSyntax.value;
                this.schemaSyntax.value = this.createSyntax.value;
            }
        }
        this.savedCreateTable = this.createSyntax.value;
    }

    schemaSyntaxChange() {
        if (this.generatedSchema !== this.schemaSyntax.value) {
            this.regenerateButtonSchema.classList.remove('hidden');
        }
        else {
            this.regenerateButtonSchema.classList.add('hidden');
        }
        if (this.savedSchema !== this.schemaSyntax.value) {
            this.fireEvent('schema-syntax-change');
        }
        this.savedSchema = this.schemaSyntax.value;
    }

    regenerateCreate() {
        this.createSyntax.value = this.generateCreateTable();
        this.regenerateButtonCreate.classList.add('hidden');
        this.fireEvent('regenerate-create-table');
    }

    regenerateSchema() {
        this.schemaSyntax.value = this.generateCreateTable('schema');
        this.regenerateButtonSchema.classList.add('hidden');
        this.fireEvent('regenerate-schema');
    }

    hide() {
        this.schemaWrapDiv.classList.add('hidden');
        this.hidden = true;
    }

    show() {
        this.schemaWrapDiv.classList.remove('hidden');
        this.hidden = false;
    }

    isHidden() {
        return this.hidden;
    }

    reset() {
        const childNodes = this.fieldsDiv.childNodes;
        const childNodesArr = Array.from(childNodes);
        childNodesArr.forEach(item => {
            if (item.classList && item.classList.contains('heading')) {
                return;
            }
            this.fieldsDiv.removeChild(item);
        });

        this.name = null;
        this.schemaName = null;
        this.engine = null;
        this.database = null;
        this.setShowTypes(false);
        this.setShowSchema(false);
        this.generatedCreateTable = null;
        this.generatedSchema = null;
        this.createSyntax.value = null;
        this.engineType = null;
    }

    setShowTypes(flag) {
        this.showTypes = flag;
    }

    setDatabase(database) {
        this.database = database;
    }

    setShowSchema(flag) {
        this.showSchema = flag;
    }

    setDefaultTypes() {
        this.types = {};
        this.types['postgres'] = {
            varchar: "character varying(%s)",
            integer: "integer",
            bigint: "bigint",
            float: "double precision"
        };
        this.types['mysql'] = {
            varchar: "varchar(%s)",
            integer: "integer",
            bigint: "bigint",
            float: "double",
        };
        this.types['vertica'] = {
            varchar: "varchar",
            text: "long varchar",
            bigint: "bigint",
            integer: "integer",
            float: "float"
        };
    }

    setEngine(engine) {
        this.engine = engine;
        const connectionProperties = engine['connection_properties'];
        if (connectionProperties.match(/^postgres/i)) {
            this.setShowTypes(true);
            this.setShowSchema(true);
            this.engineType = 'postgres';
            return;
        }
        else if (connectionProperties.match(/^mysql/i)) {
            this.setShowTypes(true);
            this.setShowSchema(true);
            this.engineType = 'mysql';
            return;
        }
        else if (connectionProperties.match(/^vertica/i)) {
            this.setShowTypes(true);
            this.setShowSchema(true);
            this.engineType = 'vertica';
            return;
        }
        this.setShowTypes(false);
        this.setShowSchema(false);
    }

    getEngineType() {
        return this.engineType;
    }

    setName(name) {
        this.name = name;
        this.regenerateCreate();
        this.regenerateSchema();
    }

    setCSV(csv, headerRow) {
        if (headerRow) {
            this.setHeaders(csv[0]);
            this.setTypes(Array.prototype.slice.call(csv, 1));
        }
        else {
            const headers = [];
            for (let i = 0; i < csv[0].length; i++) {
                headers.push(null);
            }
            this.setHeaders(headers);
            this.setTypes(csv);
        }

        this.generatedCreateTable = this.generateCreateTable();
        this.createSyntax.value = this.generatedCreateTable;
        const engineType = this.getEngineType();
        if (engineType === 'postgres') {
            this.generatedSchema = this.generatedCreateTable;
            this.schemaSyntax.value = this.generatedCreateTable;
        }
        else {
            this.generatedSchema = this.generateCreateTable('schema');
            this.schemaSyntax.value = this.generatedSchema;
        }
        if (this.showSchema) {
            this.csvCreateSchemaWrapEle.classList.remove('hidden');
            if (engineType === 'postgres') {
                this.createSchemaDiv.classList.add('hidden');
            }
            else {
                this.createSchemaDiv.classList.remove('hidden');
            }
        }
        else {
            this.csvCreateSchemaWrapEle.classList.add('hidden');
        }
    }

    setTypes(csv) {
        const rowLen = csv[0].length;
        const typeList = [];
        const schemaTypeList = [];
        for (let i = 0; i < rowLen; i++) {
            typeList.push(this.determineType(csv, i, this.getEngineType()));
            schemaTypeList.push(this.determineType(csv, i, 'postgres'));
        }

        const columnTypes = this.fieldsDiv.querySelectorAll('input.column-type');
        const schemaColumnTypes = this.fieldsDiv.querySelectorAll('input.column-schema-type');
        const spanTypes = this.fieldsDiv.querySelectorAll('span.type');
        const spanSchemaTypes = this.fieldsDiv.querySelectorAll('span.schema-type');
        const engineType = this.getEngineType();
        for (let i = 0, len = columnTypes.length; i < len ; i++) {
            const value = typeList[i];
            const schemaValue = schemaTypeList[i];

            const columnTypeEle = columnTypes[i];
            const columnTypeSpan = spanTypes[i];
            const schemaColumnTypeEle = schemaColumnTypes[i];
            const schemaColumnTypeSpan = spanSchemaTypes[i];

            if (!this.showTypes) {
                columnTypeSpan.style.visibility = 'hidden';
                schemaColumnTypeSpan.style.visibility = 'hidden';
                columnTypeEle.value = null;
                schemaColumnTypeEle.value = null;
                return;
            }
            else if (engineType === 'postgres') {
                schemaColumnTypeSpan.style.visibility = 'hidden';
            }
            else {
                schemaColumnTypeSpan.style.visibility = 'visible';
                columnTypeSpan.style.visibility = 'visible';
            }

            columnTypeEle.value = value;
            schemaColumnTypeEle.value = schemaValue;
            this.colorElement(columnTypeEle);
            this.colorElement(schemaColumnTypeEle);
        }
        if (!this.showTypes) {
            document.querySelector('.heading.schema-type-heading').style.visibility = 'hidden';
            document.querySelector('.heading.type-heading').style.visibility = 'hidden';
        }
        else if (engineType === 'postgres') {
            document.querySelector('.heading.schema-type-heading').style.visibility = 'hidden';
        }
        else {
            document.querySelector('.heading.schema-type-heading').style.visibility = 'hidden';
            document.querySelector('.heading.type-heading').style.visibility = 'hidden';
        }
    }

    setSchemaName(schemaName) {
        this.schemaName = schemaName;
        this.regenerateSchema();
        this.regenerateCreate();
    }

    generateCreateTable(type) {
        let typesList = this.getTypesList();
        const fieldsList = this.getFieldsList();
        let tableName = this.name;
        const connectionProperties = this.engine['connection_properties'];
        let prefixSchemaName = false;
        if (type === 'schema') {
            prefixSchemaName = true;
            typesList = this.getSchemaTypesList();
        }
        const engineType = this.getEngineType();
        if (engineType === 'postgres' || connectionProperties.match(/^vertica/i)) {
            prefixSchemaName = true;
        }
        if (prefixSchemaName && this.schemaName !== "" && this.schemaName !== undefined && this.schemaName !== null) {
            tableName = this.schemaName + '.' + tableName;
        }

        const prefix = "CREATE TABLE " + tableName + " (\n";

        const finalFields = [];
        for (let i = 0, fieldsListLen = fieldsList.length; i < fieldsListLen ; i++) {
            let field = fieldsList[i];
            if (typesList[i] !== undefined && typesList[i] !== null && typesList[i] !== "") {
                field += " " + typesList[i];
            }
            finalFields.push(field);
        }
        return prefix + "    " + finalFields.join(",\n    ") + "\n);";
    }

    determineType(csv, column, engineType) {
        let type = null;
        const defaultSize = 5;
        const intSignedMax = 2147483647;
        const intSignedMaxLength = ("" + intSignedMax).length;
        let maxSize = 0;

        for (let i = 0 ; i < csv.length ; i++) {
            const length = csv[i][column].length;
            if (length > maxSize) {
                maxSize = length;
            }
        }
        if (maxSize === 0) {
            maxSize = defaultSize;
        }

        for (let i = 0; i < csv.length; i++) {
            if (type === 'text') {
                break;
            }

            let curType = null;
            const value = csv[i][column];
            if (value.match(/^-?\d+$/)) {
                const parsedInt = parseInt(value);
                if (value.length >= intSignedMaxLength) {
                    if (parsedInt >= Number.MAX_SAFE_INTEGER || parsedInt <= Number.MIN_SAFE_INTEGER) {
                        curType = 'bigint';
                    }
                }
                else if (parsedInt > intSignedMax || parsedInt < -intSignedMax) {
                    curType = 'bigint';
                }
                else {
                    curType = 'integer';
                }
            }
            else if (value.match(/^\d+\.\d+$/)) {
                curType = 'float';
            }
            else {
                if (value.length <= 256) {
                    curType = 'varchar';
                }
                else {
                    curType = 'text';
                }
            }
            if (type === curType) {
                continue;
            }
            if (curType === 'text') {
                type = 'text';
                continue;
            }

            if (type === null) {
                type = curType;
                continue;
            }

            if (value.length <= 256) {
                type = 'varchar';
            }
            else {
                type = 'text';
            }
        }

        if (!this.types[engineType]) {
            return null;
        }

        if (!this.types[engineType][type]) {
            return null;
        }

        return this.types[engineType][type].replace('%s', maxSize);
    }

    headersReady() {
        return this.checkColumnRowFilled('input.column-name');
    }

    checkColumnRowFilled(inputSelector) {
        const inputs = this.fieldsDiv.querySelectorAll(inputSelector);
        let filled = true;
        inputs.forEach((item) => {
            if (filled && (item.value === "" || item.value === null)) {
                filled = false;
            }
        });
        return filled;

    }

    typesReady() {
        if (!this.showTypes) {
            return true;
        }

        return this.checkColumnRowFilled('input.column-type');
    }

    getFieldsList() {
        return this.getListFromColumns('name');
    }

    getSchemaTypesList() {
        return this.getListFromColumns('schema-type');
    }

    getListFromColumns(type) {
        const inputs = this.fieldsDiv.querySelectorAll('input.column-' + type);
        const list = [];
        inputs.forEach((item) => {
            list.push(item.value);
        });
        return list;

    }

    getTypesList() {
        return this.getListFromColumns('type');
    }

    colorElement(element) {
        if (element.value !== "" && element.value !== null && element.value !== undefined) {
            element.style.borderColor = null;
        }
        else {
            element.style.borderColor = 'red';
        }
    }

    inputOnChange(e) {
        const element = e.target;
        this.colorElement(element);
        this.generatedCreateTable = this.generateCreateTable();
        this.createSyntax.value = this.generatedCreateTable;

        if (this.getEngineType() === 'postgres') {
            this.generatedSchema = this.generatedCreateTable;
            this.schemaSyntax.value = this.generatedCreateTable;
        }
        else {
            this.generatedSchema = this.generateCreateTable('schema');
            this.schemaSyntax.value = this.generatedSchema;
        }
        this.fireEvent('input-change');
    }

    setHeaders(headers) {
        let currentIndex = 0;
        const inputNames = this.fieldsDiv.querySelectorAll('input.column-name');
        const spanColumnNums = this.fieldsDiv.querySelectorAll('span.column-num');
        const columnTypes = this.fieldsDiv.querySelectorAll('input.column-type');
        const spanTypes = this.fieldsDiv.querySelectorAll('span.type');
        const spanSchemaTypes = this.fieldsDiv.querySelectorAll('span.schema-type');
        const schemaColumnTypes = this.fieldsDiv.querySelectorAll('input.column-schema-type');

        headers.forEach((name) => {
            const prevIndex = currentIndex;
            currentIndex += 1;
            if (prevIndex < inputNames.length) {
                const columnNameEle = inputNames[prevIndex];
                if (columnNameEle.value !== name) {
                    columnNameEle.value = name;
                }
                const columnTypeEle = columnTypes[prevIndex];
                const schemaTypeEle = schemaColumnTypes[prevIndex];
                const columnTypeSpan = spanTypes[prevIndex];

                if (!this.showTypes) {
                    columnTypeSpan.style.visibility = 'hidden';
                    columnTypeEle.value = null;
                    spanSchemaTypes[prevIndex].style.visibility = 'hidden';
                    schemaTypeEle.value = null;
                }
                else if (this.getEngineType() === "postgres") {
                    spanSchemaTypes[prevIndex].style.visibility = 'hidden';
                }
                else {
                    spanSchemaTypes[prevIndex].style.visibility = 'visibile';
                    columnTypeSpan.style.visibility = 'visible';
                }

                this.colorElement(columnNameEle);
                this.colorElement(columnTypeEle);
                this.colorElement(schemaTypeEle);
                return;
            }

            const column = document.importNode(this.template.content.querySelector('.column-row'), true);
            const columnNameEle = column.querySelector('.column-name');
            columnNameEle.addEventListener('keyup', this.inputOnChange.bind(this));
            columnNameEle.addEventListener('change', this.inputOnChange.bind(this));
            const columnNumSpan = column.querySelector('.column-num');
            const columnTypeEle = column.querySelector('.column-type');
            columnTypeEle.addEventListener('change', this.inputOnChange.bind(this));
            columnTypeEle.addEventListener('keyup', this.inputOnChange.bind(this));
            const columnTypeSpan = column.querySelector('span.type');
            const schemaColumnTypeSpan = column.querySelector('span.schema-type');
            const schemaColumnTypeEle = column.querySelector('.column-schema-type');
            schemaColumnTypeEle.addEventListener('change', this.inputOnChange.bind(this));
            schemaColumnTypeEle.addEventListener('keyup', this.inputOnChange.bind(this));

            const label = column.querySelector('label');
            columnNameEle.value = name;
            columnNumSpan.innerText = currentIndex + ': ';
            columnNameEle.id = columnNameEle.name = 'column-name-' + currentIndex;
            label.for = columnNameEle.id;
            columnTypeEle.id = columnTypeEle.name = 'column-type-' + currentIndex;
            if (!this.showTypes) {
                columnTypeSpan.style.visibility = 'hidden';
                schemaColumnTypeSpan.style.visibility = 'hidden';
                columnTypeEle.value = null;
                schemaColumnTypeEle.value = null;
            }
            else if (this.getEngineType() === "postgres") {
                schemaColumnTypeSpan.style.visibility = 'hidden';
            }

            const childNodes = column.childNodes;
            const childNodesArr = Array.from(childNodes);
            childNodesArr.forEach(item => this.fieldsDiv.appendChild(item));
        });

        if (inputNames.length > currentIndex) {
            // need to prune
            for (let i = currentIndex; i < inputNames.length; i++) {
                this.fieldsDiv.removeChild(inputNames[i].parentNode);
                this.fieldsDiv.removeChild(spanTypes[i]);
                this.fieldsDiv.removeChild(spanSchemaTypes[i]);
                this.fieldsDiv.removeChild(spanColumnNums[i]);
            }
        }

        const inputNamesArr = Array.from(this.fieldsDiv.querySelectorAll('input.column-name'));
        inputNamesArr.forEach(item => this.colorElement(item));
    }


}

class CSV {
    constructor() {
        this.csv = [];
    }

    parse(str) {
        if (this.csv) {
            this.csv = [];
        }
        this.csvLines = str.split(/\r?\n/);
        const csvLinesLen = this.csvLines.length;
        this.csvLines.forEach((line, idx) => {
            if (!line && idx === (csvLinesLen - 1)) {
                // File is ending on a new line, so skip.
                return;
            }
            this.csv.push(CSV.parseCsvLine(line));
        });
    }

    get() {
        return this.csv;
    }

    set(csv) {
        this.csv = csv;
    }

    getLines() {
        return this.csvLines;
    }

    static parseCsvLine(line) {
        let state = 'start';
        const len = line.length;

        const parsed = [];
        let currentPart = '';

        for (let i = 0; i < len; i++) {
            switch (line[i])
            {
                case ',':
                    if (state !== 'quote') {
                        parsed.push(currentPart);
                        currentPart = '';
                    }
                    break;
                case '"':
                    if (state === 'quote') {
                        if (i >= 0 && line[i - 1] === '"') {
                            currentPart += '"';
                        }
                        break;
                    }
                    state = 'quote';
                    break;
                default:
                    currentPart += line[i];
            }

        }
        parsed.push(currentPart);
        return parsed;
    }
}

window.addEventListener('load', () => {
    const chooser = new Chooser();
});

