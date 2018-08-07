class ApiForm extends Evented {
    constructor() {
        super();
        this.errorElement = document.getElementById('error');
        this.successElement = document.getElementById('success');
        this.resetButton = document.getElementById('reset');

        this.api = new Api('api');
        this.endpoint = new Endpoint('endpoint');
        this.auth = new Auth();
        this.advanced = new Advanced();
        this.addEventListeners();
        this.toggle();
        this.api.reset();
        this.advanced.reset();
        this.endpoint.reset();
        this.auth.reset();
    }

    setEngines(engines) {
        this.endpoint.setEngines(engines);
    }

    setParameters(apiParameters, endpointParameters) {
        this.endpoint.setParameters(endpointParameters);
        this.api.setParameters(apiParameters);
        this.auth.setParameters(apiParameters);
    }

    addEventListeners() {
        document.querySelectorAll('input[name=type]').forEach(
            (element) => {
                element.addEventListener('change', this.toggle.bind(this));
            });

        document.querySelector('form').addEventListener('submit', this.submit.bind(this));
        document.getElementById('name').addEventListener('keyup', this.checkApiName.bind(this));
        this.resetButton.addEventListener('click', this.reset.bind(this));
    }

    toggle() {
        document.querySelectorAll('input[name=type]').forEach(
            (element) => {
                if (element.checked) {
                    switch(element.value) {
                        case "api":
                            this.showApi();
                            break;
                        case "endpoint":
                            this.api.hide();
                            this.auth.hide();
                            this.advanced.deepHide();
                            this.endpoint.show();
                            break;
                        case "api+endpoint":
                            this.showApiEndpoint();
                            break;
                        default:
                            throw "Unknown element";
                    }
                    this.showForm();
                }
            }
        );
    }

    showForm() {
        document.querySelector('form').classList.remove("hidden");
        this.hideSuccess();
        this.hideError();
        ApiForm.showButton();
    }

    hideForm() {
        document.querySelector('form').classList.add("hidden");
        ApiForm.hideButton();
    }

    static showButton() {
        document.querySelector('.submit').classList.remove('hidden')
    }

    static hideButton() {
        document.querySelector('.submit').classList.add('hidden')
    }

    showApi() {
        this.api.show();
        this.auth.toggleAuth();
        this.endpoint.hide();
        this.advanced.hide();
    }

    showApiEndpoint() {
        this.showApi();
        this.endpoint.show('not-api');
    }

    static convertConnectionProperties(connectionProperties) {
        let connectionPropertiesStr = "";
        connectionProperties.forEach((value, key) => {
            if (connectionPropertiesStr) {
                connectionPropertiesStr += ',';
            }
            connectionPropertiesStr += key + '=' + encodeURIComponent(value);
        });
        return connectionPropertiesStr;
    }

    static parseConnectionProperties(connectionPropertiesStr) {
        if (!connectionPropertiesStr || connectionPropertiesStr.length === 0) {
            return new Map();
        }

        const pairs = connectionPropertiesStr.split(',');
        const map = new Map();
        pairs.forEach(pair => {
            const pairParts = pair.split('=');
            map.set(pairParts[0], decodeURIComponent(pairParts[1]));
        });
        return map;
    }

    getFormData() {
        const api = this.api.getVisibleFormData();
        const endpoint = this.endpoint.getVisibleFormData();

        if (api.size > 0) {
            this.auth.getFormData(api);
        }

        const data = {};
        api.forEach((value, key) => {
            if (value instanceof Map) {
                return;
            }
            if (!data['api']) {
                data['api'] = {};
            }
            data['api'][key] = value;
        });

        if (api.has('connection_properties')) {
            data['api']['connection_properties'] = 'REST:' + ApiForm.convertConnectionProperties(api.get('connection_properties'));
        }

        endpoint.forEach((value, key) => {
            if (!data['endpoint']) {
                data['endpoint'] = {};
            }
            data['endpoint'][key] = value;
        });

        if (endpoint.has('password_field')) {
            data['endpoint']['password_field'] = ApiForm.convertConnectionProperties(endpoint.get('password_field'));
        }

        if (endpoint.has(url) && api.size) {
            const aElement = document.createElement('a');
            aElement.href = endpoint.get(url);
            const host = aElement.hostname;
            let port = aElement.port;
            if (port) {
                port = parseInt(port.toString());
            }
            if (!port) {
                if (aElement.protocol.match(/^http:/)) {
                    port = 80;
                }
                else if (aElement.protocol.match(/^https:/)) {
                    port = 443;
                }
                else {
                    alert("Unknown scheme type: " + aElement.protocol);
                    return;
                }
            }

            if (host) {
                data['api']['host'] = host;
                data['api']['port'] = port;
            }
        }

        return data;
    }

    showError(msg) {
        this.errorElement.innerText = msg;
        this.errorElement.classList.remove('hidden');
    }

    hideError() {
        this.errorElement.innerText = '';
        this.errorElement.classList.add('hidden');
    }

    showSuccess(msg) {
        this.successElement.innerText = msg;
        this.successElement.classList.remove('hidden');
        document.querySelector('form').classList.add("hidden");
    }

    hideSuccess() {
        this.successElement.innerText = '';
        this.successElement.classList.add('hidden');
    }

    static disableForm() {
        document.querySelectorAll('input').forEach((element) => {
           element.disabled = true;
        });
        document.querySelectorAll('select').forEach((element) => {
           element.disabled = true;
        });
        document.querySelectorAll('button').forEach((element) => {
           element.disabled = true;
        });
    }

    static enableForm() {
        document.querySelectorAll('input').forEach((element) => {
           element.disabled = false;
        });
        document.querySelectorAll('select').forEach((element) => {
           element.disabled = false;
        });
        document.querySelectorAll('button').forEach((element) => {
           element.disabled = false;
        });
    }

    static hideLoading() {
        document.getElementById('add-loading').style.display='none';
    }

    static showLoading() {
        document.getElementById('add-loading').style.display='block';
    }

    static uncheckType() {
        document.querySelectorAll('input[name=type]').forEach(element => {
           if (element.checked) {
               element.checked = false;
           }
        });
    }

    showApiNameDuplication() {
        const span = document.getElementById('name').parentNode.parentNode.querySelector('span.error');
        span.style.display="inline";
        span.innerText = "Error: duplicate name";
    }

    hideApiNameDuplication() {
        const span = document.getElementById('name').parentNode.parentNode.querySelector('span.error');
        span.style.display="none";
        span.innerText = "";
    }

    checkApiName() {
        const name = document.getElementById('name').value;
        this.currentNameCheck = name;
        const checkNameFunc = (name) => {
            fetchJson("/get_engine_by_name", JSON.stringify({name: name})).then(
                result => {
                    if (this.currentNameCheck === name) {
                        this.showApiNameDuplication();
                    }
                },
                reject => {
                }
            );
        };

        this.hideApiNameDuplication();

        setTimeout(() => {
            if (this.currentNameCheck === name) {
                checkNameFunc(this.currentNameCheck);
            }
        }, 500);
    }

    submit(e) {
        e.preventDefault();
        e.stopPropagation();

        ApiForm.showLoading();
        ApiForm.disableForm();
        this.hideError();
        const formData = this.getFormData();

        if (formData === undefined || formData === null) {
            ApiForm.hideLoading();
            ApiForm.enableForm();
            return false;
        }

        fetchJson('/api_form', JSON.stringify(formData)).then(result => {
            ApiForm.hideLoading();
            ApiForm.enableForm();
            this.reset();
            this.api.hide();
            this.auth.hide();
            this.endpoint.hide();
            this.advanced.hide();
            ApiForm.hideButton();
            ApiForm.uncheckType();
            this.showSuccess('Success');
            this.fireEvent('add');
        }, msg => {
            ApiForm.hideLoading();
            ApiForm.enableForm();
            this.showError(msg);
            console.log(msg);
        });
        return false;
    }

    reset() {
        this.api.reset();
        this.auth.reset();
        this.endpoint.reset();
        this.advanced.reset();
        this.fireEvent('reset');
    }

    static getInputPair(pairs, input, key, value) {
        if (input.dataset && input.dataset.type) {
            const type = input.dataset.type;
            if (!pairs.has(type)) {
                pairs.set(type, new Map());
            }
            const typeMap = pairs.get(type);
            typeMap.set(key, value);
            return;
        }

        pairs.set(key, value);
    }

    static collectFormData(element, pairs = new Map()) {
        element.querySelectorAll('input').forEach((input) => {
            if (input.value !== "" && input.value !== null) {
                if (input.type !== 'radio' || input.checked) {
                    let name = input.name;
                    if (input.dataset) {
                        if (input.dataset.name) {
                            name = input.dataset.name;
                        }

                        if (input.dataset.skipGet) {
                            return;
                        }
                    }

                    ApiForm.getInputPair(pairs, input, name, input.value);
                }
            }
        });
        return pairs;
    }

    static setFormData(element, data) {
        element.querySelectorAll('input').forEach((input) => {
            let name = input.name;
            if (input.dataset && input.dataset.name) {
                name = input.dataset.name;
            }
            ApiForm.setInputPair(data, input, name, input.value);
        });
    }

    static setInputPair(data, input, key, value) {
        if (input.dataset && input.dataset.type) {
            const type = input.dataset.type;
            if (!data.has(type)) {
                return;
            }
            const typeMap = data.get(type);
            if (typeMap.has(key)) {
                const value = typeMap.get(key);
                if (input.type === 'radio' || input.type === 'checkbox') {
                    if (value === input.value) {
                        input.checked = true;
                    }
                }
                else {
                    input.value = typeMap.get(key);
                }
            }
            return;
        }

        if (data.has(key)) {
            input.value = data.get(key);
        }
    }


    static resetSubElements(element) {
        element.querySelectorAll('input[type=text]').forEach(item => {
            item.value = "";
        });
        element.querySelectorAll('input[type=hidden]').forEach(item => {
            item.value = "";
        });
        element.querySelectorAll('input[type=url]').forEach(item => {
            item.value = "";
        });
        element.querySelectorAll('select').forEach(item => {
            item.selectedIndex = 0;
        });
        const radio = element.querySelector('input[type=radio]');
        if (radio) {
            radio.checked = false;
        }
        element.querySelectorAll('input[type=checkbox]').forEach(item => {
            item.checked = false;
        });
    }
}

class ApiList extends Evented {
    constructor() {
        super();
        this.content = document.querySelector("div#list .content");
        this.tableTemplate = document.getElementById("api-template");
    }

    showLoading() {
        document.getElementById('content-loading').style.display='block';
        const childNodes = Array.from(this.content.childNodes);
        childNodes.forEach(element => this.content.removeChild(element));
    }

    load() {
        this.showLoading();
        fetchJson("/api_list", {}, "get").then(result => {
            document.getElementById('content-loading').style.display='none';
            this.fireEvent('api_list', result);
            const engines = result.engines;
            const databases = result.databases;
            if (!engines || !databases || engines.length === 0 || databases.length === 0) {
                this.showNone();
                return;
            }
            const enginesById = new Map();
            for (let i = 0, len = engines.length; i < len; i++) {
                enginesById.set(engines[i][0], engines[i]);
            }
            const table = document.importNode(this.tableTemplate.content, true);
            const tbody = table.querySelector('tbody');
            databases.forEach(database => {
                const tr = document.createElement('tr');
                const td1 = document.createElement('td');
                const td2 = document.createElement('td');
                const td3 = document.createElement('td');
                const td4 = document.createElement('td');
                const td5 = document.createElement('td');
                const td6 = document.createElement('td');
                const parameters = ApiForm.parseConnectionProperties(database[4]);
                const editButton = document.createElement('button');
                td1.classList.add('buttons');
                editButton.dataset.dbid = database[0];
                editButton.addEventListener('click', this.edit.bind(this));
                editButton.classList.add("btn");
                editButton.classList.add("btn-primary");
                editButton.classList.add("btn-sm");
                editButton.appendChild(document.createTextNode("Edit"));
                td1.appendChild(editButton);
                const deleteButton = document.createElement('button');
                deleteButton.dataset.dbid = database[0];
                deleteButton.addEventListener('click', this.delete.bind(this));
                deleteButton.classList.add("btn");
                deleteButton.classList.add("btn-sm");
                deleteButton.appendChild(document.createTextNode("Delete"));
                td1.appendChild(deleteButton);
                const engine = enginesById.get(database[1]);
                td2.innerText = engine[1];
                td3.innerText = database[2];
                let requiredParams = null;
                if (parameters.has('required_params')) {
                    requiredParams = AddElement.parseParameters(parameters.get('required_params'));
                    ApiList.appendList(requiredParams, td4);
                }
                td6.appendChild(document.createTextNode(ApiList.generateSampleQuery(engine[1], database[2], requiredParams)));
                if (parameters.has('optional_params')) {
                    const optionalParams = AddElement.parseParameters(parameters.get('optional_params'));
                    ApiList.appendList(optionalParams, td5);
                }
                tr.appendChild(td1);
                tr.appendChild(td2);
                tr.appendChild(td3);
                tr.appendChild(td4);
                tr.appendChild(td5);
                tr.appendChild(td6);
                tbody.appendChild(tr);
            });
            this.content.appendChild(table);
        }, msg => { alert(msg); });
    }

    static generateSampleQuery(api, endpoint, requiredParams) {
        let queryStr = "bdapi({ 'name' : '" + api + "', ";
        queryStr += "'endpoint' : '" + endpoint + "'";
        if (requiredParams) {
            queryStr += ", 'query': { ";
            let first = true;
            let counter = 1;
            requiredParams.forEach(param => {
                if (first) {
                    first = false;
                }
                else {
                    queryStr += ", ";
                }
                queryStr += "'" + param + "' : '";
                queryStr += "abc" + parseInt(counter++);
                queryStr += "'";
            });
            queryStr += " }";
        }
        queryStr += " })";
        return queryStr;
    }

    edit(e) {
        const button = e.target;
        const dbid = button.dataset.dbid;
        this.fireEvent('edit', dbid);
    }

    delete(e) {
        const button = e.target;
        const dbid = button.dataset.dbid;
        this.fireEvent('delete', dbid);
    }

    static appendList(list, element) {
        let first = true;
        list.forEach(item => {
            if (first) {
                first = false;
            }
            else {
                element.appendChild(document.createElement("br"));
            }
            element.appendChild(document.createTextNode(item));
        });
    }

    showNone() {
        this.content.innerHTML = "<p><em>None found.</em></p>";
    }
}

class Advanced {
    constructor() {
        this.addEventListeners();
    }

    setParameters(parameters) {
        document.querySelectorAll(`.advanced`).forEach((element) => {
            if (element.querySelector('.query_params')) {
                return;
            }
            ApiForm.setFormData(element, parameters);
        });

        if (parameters.has('connection_properties') && parameters.get('connection_properties').has('query_params')) {

        }
        const queryParams = this.queryParams.getFormData();
        if (queryParams) {
            pairs.set('query_params', queryParams);
        }
        return pairs;

    }

    reset() {
        document.querySelectorAll('.advanced').forEach((item) => {
            ApiForm.resetSubElements(item);
        });
    }

    addEventListeners() {
        document.getElementById('advanced-link').addEventListener('click', this.toggleAdvanced.bind(this));
    }

    show() {
        document.querySelectorAll('.advanced').forEach((item) => {
            item.classList.remove("hidden");
        });
        document.getElementById('advanced-link').innerText = "Advanced <<";
        document.getElementById('advanced-link').style.visibility = 'visible';
    }

    hide() {
        document.querySelectorAll('.advanced').forEach((item) => {
            item.classList.add("hidden");
        });
        document.getElementById('advanced-link').innerText = "Advanced >>";
        document.getElementById('advanced-link').style.visibility = 'visible';
    }

    deepHide() {
        this.hide();
        document.getElementById('advanced-link').style.visibility = 'hidden';
    }

    toggleAdvanced() {
        if (document.getElementById('advanced-link').innerText === "Advanced >>") {
            this.show();
            return;
        }
        this.hide();
    }
}

class FormType {
    constructor(className) {
        this.className = className;
    }

    setParameters(parameters) {
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            ApiForm.setFormData(element, parameters);
            element.querySelectorAll('select').forEach((select) => {
                if (parameters.has(select.name)) {
                    const value = parameters.get(select.name);
                    const options = Array.from(select.options);
                    options.forEach((option, idx) => {
                        if (option.value === value) {
                            select.selectedIndex = idx;
                        }
                    });
                }
            });
        })
    }

    hide() {
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            element.querySelectorAll('.required').forEach((item) => {
               item.required = false;
            });
            element.classList.add('hidden');
        });

    }

    reset() {
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            ApiForm.resetSubElements(element);
        });
    }

    getVisibleFormData(pairs = new Map()) {
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            if (element.classList.contains('hidden')) {
                return;
            }
            ApiForm.collectFormData(element, pairs);
            element.querySelectorAll('select').forEach((select) => {
                if (select.selectedIndex > -1) {
                    const selectedValue = select.selectedOptions[0].value;
                    if (selectedValue !== "" && selectedValue !== null) {
                        pairs.set(select.name, selectedValue);
                    }
                }
            });
        });
        return pairs;
    }

    show(skipClass) {
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            if (skipClass && element.classList.contains(skipClass)) {
                return;
            }
            element.querySelectorAll('.required').forEach((item) => {
               item.required = true;
            });
            element.classList.remove('hidden');
        });
    }
}

class Api extends FormType {
    constructor(className) {
        super(className);
        this.postEncodingDiv = document.querySelector('.post-encoding');
        this.postInput = document.querySelector('.method').querySelector('input[value=POST]');
        this.addEventListeners();
    }

    addEventListeners() {
        document.querySelectorAll('input[name=method]').forEach(input => {
            input.addEventListener('change', this.togglePostEncoding.bind(this));
        });
    }

    reset() {
        super.reset();
        this.togglePostEncoding();
    }

    togglePostEncoding() {
        if (this.postInput.checked) {
            this.postEncodingDiv.classList.remove('hidden');
            this.postEncodingDiv.querySelectorAll('input').forEach(item => {
                item.required = true;
            });
            return;
        }
        this.postEncodingDiv.classList.add('hidden');
        this.postEncodingDiv.querySelectorAll('input').forEach(item => {
            item.required = false;
            item.checked = false;
        });
    }

    show(skipClass) {
        super.show(skipClass);
        const checked = this.postInput.checked;
        this.togglePostEncoding();
    }
}

class Endpoint extends FormType {
    constructor(className) {
        super(className);
        this.requiredParams = new AddElement('required_param');
        this.optionalParams = new AddElement('optional_param');
        this.queryParams = new KeyValueParameters('query_params', 'query_params', 'Extra Query Parameters');
        this.enginesSelect = document.getElementById('engine_id');
        this.engineLoading = document.getElementById('engine-loading');
        this.showLoading();
    }

    hideLoading() {
        this.engineLoading.style.display='none';
    }

    showLoading() {
        this.engineLoading.style.display='block';
    }

    setEngines(engines) {
        const options = Array.from(this.enginesSelect.options);
        for (let i = 1, optionsLen = options.length; i < optionsLen; i++) {
            this.enginesSelect.removeChild(options[i]);
        }
        engines.forEach(engine => {
            const optionElement = document.createElement('option');
            optionElement.value = engine[0];
            optionElement.appendChild(document.createTextNode(engine[1] + " "));
            const em = document.createElement('em');
            em.appendChild(document.createTextNode("(engine_id: " + engine[0] + ")"));
            this.enginesSelect.appendChild(optionElement);
        });
        this.hideLoading();
        this.enginesSelect.classList.remove('hidden');
    }

    setParameters(parameters) {
        super.setParameters(parameters);
        if (parameters.has('password_field')) {
            const passwordField = parameters.get('password_field');
            if (passwordField.has('required_params')) {
                this.requiredParams.setParameters(passwordField.get('required_params'));
            }
            if (passwordField.has('optional_params')) {
                this.optionalParams.setParameters(passwordField.get('optional_params'));
            }
            if (passwordField.has('query_params')) {
                this.queryParams.setParameters(passwordField.get('query_params'));
            }
        }
    }

    reset() {
        super.reset();
        this.queryParams.reset();
        this.requiredParams.reset();
        this.optionalParams.reset();
    }

    hide() {
        super.hide();
        this.queryParams.hideParameters();
    }

    show(skip) {
        super.show(skip);
        this.queryParams.toggleParameters();
    }

    getVisibleFormData(pairs = new Map()) {
        pairs = super.getVisibleFormData(pairs);
        const requiredParamsData = this.requiredParams.getFormData();
        const optionalParamsData = this.optionalParams.getFormData();
        pairs.set('password_field', new Map());
        const passwordFieldMap = pairs.get('password_field');
        if (requiredParamsData && requiredParamsData.length) {
            passwordFieldMap.set('required_params', requiredParamsData);
        }

        if (optionalParamsData && optionalParamsData.length) {
            passwordFieldMap.set('optional_params', optionalParamsData);
        }

        const queryParams = this.queryParams.getFormData();
        if (queryParams) {
            passwordFieldMap.set('query_params', queryParams);
        }
        return pairs;
    }
}

class AddElement {
    constructor(className) {
        this.className = className;
        this.addEventListeners();
    }

    addEventListeners() {
        document.querySelector(`.${this.className}`).querySelector('.plus').addEventListener('click', this.add.bind(this));
    }

    static parseParameters(paramsStr) {
        if (paramsStr === undefined || paramsStr === null || paramsStr.length === "") {
            return [];
        }
        const parts = paramsStr.split(',');
        const params = [];
        parts.forEach(part => {
           params.push(decodeURIComponent(part));
        });
        return params;
    }

    static prepareParamsData(paramsArr) {
        let result = "";
        paramsArr.forEach(param => {
           if (result.length > 0) {
               result += ",";
           }
           result += param.replace(",","%2C");
        });
        return result;
    }

    setParameters(paramsStr) {
        const parameters = AddElement.parseParameters(paramsStr);
        parameters.forEach((parameter, idx) => {
            const id = `${this.className}_` + parseInt(idx + 1);
            let element = document.getElementById(id);
            if (!element) {
                this.add();
                element = document.getElementById(id);
                if (!element) {
                    throw "Can't find element " + id;
                }
            }
            element.value = parameter;
        });
    }

    getFormData() {
        const values = [];
        document.querySelectorAll(`.${this.className} input[type=text]`).forEach((input) => {
            if (input.value !== "" && input.value !== null) {
                values.push(input.value);
            }
        });
        return AddElement.prepareParamsData(values);
    }

    add() {
        let currentNum = 0;
        document.querySelectorAll(`.${this.className} input`).forEach((item) => {
            const regex = new RegExp(`${this.className}_(\\d+)`);
            const match = item.id.match(regex);
            if (match)
            {
                const num = parseInt(match[1]);
                if (num > currentNum) {
                    currentNum = num;
                }
            }
        });
        currentNum++;
        const parameterElements = document.querySelectorAll(`.${this.className}`);

        let html = parameterElements[parameterElements.length - 1].innerHTML;
        const regexKey = new RegExp(`${this.className}_\\d+`, 'g');
        html = html.replace(regexKey, `${this.className}_${currentNum}`);
        if (parameterElements.length === 1) {
            html += ' <span class="minus">&ndash;</span>';
        }
        const divElement = document.createElement('div');
        divElement.classList.add('form-group');
        divElement.classList.add(this.className);
        divElement.innerHTML = html;
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            const plus = element.querySelector('.plus');
            if (plus) {
                element.removeChild(plus);
            }
        });
        document.querySelector(`.${this.className}`).parentNode.appendChild(divElement);
        divElement.querySelector('.minus').addEventListener('click', this.remove.bind(this));
        divElement.querySelector('.plus').addEventListener('click', this.add.bind(this));
    }

    reset() {
        let minus = null;
        while (minus = document.querySelector(`.${this.className} .minus`)) {
            this.removeByElement(minus);
        }
        ApiForm.resetSubElements(document.querySelector(`.${this.className}`));
    }

    remove(e) {
        e.stopPropagation();
        e.preventDefault();
        const element = e.target;
        this.removeByElement(element);
    }

    removeByElement(element) {
        const parentElement = element.parentNode;
        parentElement.parentElement.removeChild(parentElement);
        const parameterElements = document.querySelectorAll(`.${this.className}`);

        const lastOne = parameterElements[parameterElements.length - 1];
        const potentialMinusElement = lastOne.childNodes[lastOne.childNodes.length - 1];
        const span = document.createElement('span');
        span.innerText = "+";
        span.classList.add('plus');
        span.addEventListener('click', this.add.bind(this));
        if (potentialMinusElement.classList && potentialMinusElement.classList.contains('minus')) {
            if (!potentialMinusElement.parentNode.querySelector('.plus')) {
                potentialMinusElement.parentNode.insertBefore(span, potentialMinusElement);
                const textNode = document.createTextNode(" ");
                potentialMinusElement.parentNode.insertBefore(textNode, potentialMinusElement);
            }
            return;
        }
        potentialMinusElement.parentNode.appendChild(span);
    }

}

class KeyValueParameters {
    constructor(checkboxName, className, text) {
        this.checkboxName = checkboxName;
        this.className = className;
        this.text = text;
        this.addEventListeners();
        this.toggleParameters();
    }

    addEventListeners() {
        document.querySelector(`input[name=${this.checkboxName}]`).addEventListener('change', this.toggleParameters.bind(this));
        document.querySelector(`.${this.className}`).querySelector('.plus').addEventListener('click', this.addPair.bind(this));
    }

    toggleParameters() {
        if (document.querySelector(`input[name=${this.checkboxName}]`).checked) {
            this.showParameters();
            return;
        }
        this.hideParameters();
    }

    showParameters() {
        document.querySelectorAll(`.${this.className}`).forEach((item) => {
           item.classList.remove('hidden');
           item.querySelectorAll('.required').forEach((element) => {
               element.required = true;
               });
        });
       document.querySelector(`label[for=${this.checkboxName}]`).innerText = `${this.text}:`;
    }

    hideParameters() {
        document.querySelectorAll(`.${this.className}`).forEach((item) => {
           item.classList.add('hidden');
           item.querySelectorAll('.required').forEach((element) => {
               element.required = false;
               });
        });
       document.querySelector(`label[for=${this.checkboxName}]`).innerText = `${this.text}?`;
    }

    getFormData() {
        const checkboxElement = document.querySelector(`input[name=${this.checkboxName}]`);
        if (checkboxElement.checked) {
            let parameters = "";
            const keyPairs = new Map();
            const valuePairs = new Map();
            checkboxElement.parentElement.parentElement.querySelectorAll('input[type=text]').forEach((input) => {
                if (input.value !== "" && input.value !== null) {
                    if (input.name.match("_key_")) {
                        keyPairs.set(input.name, input.value);
                    }
                    else if (input.name.match("_value_")) {
                        valuePairs.set(input.name, input.value);
                    }
                    else {
                        throw "Unknown type of input name: " + input.name;
                    }
                }
            });

            keyPairs.forEach((keyValue, key) => {
               const valueKey = key.replace(/_key_/, '_value_');
               if (valuePairs.has(valueKey)) {
                   const valueValue = valuePairs.get(valueKey);
                   if (parameters) {
                        parameters += '&';
                    }
                    parameters += encodeURIComponent(keyValue) + '=' + encodeURIComponent(valueValue);
               }
               else {
                   throw "Can't find value for key: " + key;
               }
            });

            return parameters;
        }
    }

    setParameters(parameters) {
        if (!parameters || parameters.length === 0) {
            return;
        }
        const pairs = parameters.split('&');

        pairs.forEach((pair, idx) => {
            const pairParts = pair.split('=');
            const key = decodeURIComponent(pairParts[0]);
            const value = decodeURIComponent(pairParts[1]);
            const keyId = `${this.className}_key_` + parseInt(idx + 1);
            const valueId = `${this.className}_value_` + parseInt(idx + 1);
            let keyElement = document.getElementById(keyId);
            if (!keyElement) {
                this.addPair();
                keyElement = document.getElementById(keyId);
                if (!keyElement) {
                    throw "Can't find element " + keyId;
                }
            }
            const valueElement = document.getElementById(valueId);
            keyElement.value = key;
            valueElement.value = value;
        });
        const checkboxElement = document.querySelector(`input[name=${this.checkboxName}]`);
        checkboxElement.checked = true;
    }


    addPair() {
        let currentNum = 0;
        document.querySelectorAll(`.${this.className} input`).forEach((item) => {
            const regex = new RegExp(`${this.checkboxName}_key_(\\d+)`);
            const match = item.id.match(regex);
            if (match)
            {
                const num = parseInt(match[1]);
                if (num > currentNum) {
                    currentNum = num;
                }
            }
        });
        currentNum++;
        const parameterElements = document.querySelectorAll(`.${this.className}`);

        let html = parameterElements[parameterElements.length - 1].innerHTML;
        const regexKey = new RegExp(`${this.checkboxName}_key_\\d+`, 'g');
        const regexValue = new RegExp(`${this.checkboxName}_value_\\d+`, 'g');
        html = html.replace(regexKey, `${this.checkboxName}_key_${currentNum}`);
        html = html.replace(regexValue, `${this.checkboxName}_value_${currentNum}`);
        if (parameterElements.length === 1) {
            html += ' <span class="minus">&ndash;</span>';
        }
        const divElement = document.createElement('div');
        divElement.classList.add('form-group');
        divElement.classList.add(this.className);
        divElement.innerHTML = html;
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            const plus = element.querySelector('.plus');
            if (plus) {
                element.removeChild(plus);
            }
        });
        document.querySelector(`.${this.className}`).parentNode.appendChild(divElement);
        divElement.querySelector('.minus').addEventListener('click', this.removePair.bind(this));
        divElement.querySelector('.plus').addEventListener('click', this.addPair.bind(this));
    }

    reset() {
        let minus = null;
        while (minus = document.querySelector(`.${this.className} .minus`)) {
            this.removePairByElement(minus);
        }
        ApiForm.resetSubElements(document.querySelector(`.${this.className}`));
    }

    removePair(e) {
        e.stopPropagation();
        e.preventDefault();
        const element = e.target;
        this.removePairByElement(element);
    }

    removePairByElement(element) {
        const parentElement = element.parentNode;
        parentElement.parentElement.removeChild(parentElement);
        const parameterElements = document.querySelectorAll(`.${this.className}`);

        const lastOne = parameterElements[parameterElements.length - 1];
        const potentialMinusElement = lastOne.childNodes[lastOne.childNodes.length - 1];
        const span = document.createElement('span');
        span.innerText = "+";
        span.classList.add('plus');
        span.addEventListener('click', this.addPair.bind(this));
        if (potentialMinusElement.classList && potentialMinusElement.classList.contains('minus')) {
            if (!potentialMinusElement.parentNode.querySelector('.plus')) {
                potentialMinusElement.parentNode.insertBefore(span, potentialMinusElement);
                const textNode = document.createTextNode(" ");
                potentialMinusElement.parentNode.insertBefore(textNode, potentialMinusElement);
            }
            return;
        }
        potentialMinusElement.parentNode.appendChild(span);
    }
}


class Auth {
    constructor() {
        this.addEventListeners();
        this.oauth2AuthResponseValidate = new KeyValueParameters('oauth2_auth_response_validate', 'oauth2_auth_response_validate', 'Auth Response Validation');
        this.toggleAuth();
        Auth.toggleOAuth2Method();
    }

    reset() {
        document.querySelector('input[name=auth_type]').checked = true;
        document.querySelectorAll('.auth').forEach(element => {
            ApiForm.resetSubElements(element);
        });
        this.oauth2AuthResponseValidate.reset();
    }

    setParameters(parameters) {
        document.querySelectorAll(`.auth`).forEach((element) => {
            const childNodesArr = Array.from(element.childNodes);
            childNodesArr.forEach((item) => {
                if (item.id === 'auth-response-validate') {
                    if (parameters.has('connection_properties') && parameters.get('connection_properties').has('auth_response_validate')) {
                        this.oauth2AuthResponseValidate.setParameters(parameters.get('connection_properties').get('auth_response_validate'));
                    }
                    return;
                }
                ApiForm.setFormData(element, parameters);
            });
        });
        Auth.toggleOAuth2Method();
    }

    addEventListeners() {
        document.querySelectorAll('input[name=auth_type]').forEach((item) => {
           item.addEventListener('change', this.toggleAuth.bind(this));
        });
        document.getElementById('oauth2_auth_response_validate').addEventListener('change', Auth.toggleOAuth2ResponseValidation);
        document.querySelectorAll('input[name=auth_method]').forEach((element) => {
            addEventListener('change', Auth.toggleOAuth2Method);
        });
    }

    toggleAuth() {
        this.hide();
        document.querySelectorAll('input[name=auth_type]').forEach((item) => {
            if (!item.checked) {
                return;
            }

            const element = document.getElementById('auth-' + item.value);
            if (element) {
                element.classList.remove('hidden');
                element.querySelectorAll('.required').forEach((item) => {
                    item.required = true;
                });
                if (item.value === "oauth2") {
                    this.oauth2AuthResponseValidate.toggleParameters();
                }
            }

        });
    }

    getFormData(pairs = new Map()) {
        document.querySelectorAll(`.auth`).forEach((element) => {
            if (element.classList.contains('hidden')) {
                return;
            }
            const childNodesArr = Array.from(element.childNodes);
            childNodesArr.forEach((item) => {
                if (!item.classList || item.classList.contains('hidden')) {
                   return;
                }

                if (item.id === 'auth-response-validate') {
                    const authParameters = this.oauth2AuthResponseValidate.getFormData();
                    if (authParameters) {
                        if (!pairs.has('connection_properties')) {
                            pairs.set('connection_properties', new Map());
                        }
                        const connectionProperties = pairs.get('connection_properties');
                        connectionProperties.set('auth_response_validate', authParameters);
                    }
                    return;
                }

                ApiForm.collectFormData(element, pairs);
            });
        });
    }


    static toggleOAuth2Method() {
        document.querySelectorAll('input[name=auth_method]').forEach((element) => {
           if (element.checked) {
               if (element.value === 'POST') {
                   Auth.showOAuth2PostData();
                   return;
               }
               Auth.hideOAuth2PostData();
           }
        });
    }

    static showOAuth2PostData() {
        document.querySelectorAll('.oauth2_post').forEach((element) => {
            element.classList.remove('hidden');
        })
    }

    static hideOAuth2PostData() {
        document.querySelectorAll('.oauth2_post').forEach((element) => {
            element.classList.add('hidden');
        })
    }

    hide() {
        document.querySelectorAll('.auth').forEach((item) => {
            item.classList.add('hidden');
            item.querySelectorAll('.required').forEach((item) => {
                item.required = false;
            });
            this.oauth2AuthResponseValidate.hideParameters();
        });
    }

    static toggleOAuth2ResponseValidation() {
        if (document.querySelector('input[name=oauth2_auth_response_validate]').checked) {
            Auth.showAuthResponse();
            return;
        }
        Auth.hideAuthResponse();
    }
    static showAuthResponse() {
        document.querySelector('.oauth2_auth_response_validate').classList.remove('hidden');
        document.querySelector('label[for=oauth2_auth_response_validate]').innerText = "Auth Response Validation:";
    }

    static hideAuthResponse() {
        document.querySelector('.oauth2_auth_response_validate').classList.add('hidden');
        document.querySelector('label[for=oauth2_auth_response_validate]').innerText = "Auth Response Validation?";
    }


}

class Master {
    constructor() {
        this.apiForm = new ApiForm();
        this.apiList = new ApiList();
        this.apiList.load();
        this.addEventListeners();
    }

    addEventListeners() {
        this.apiForm.addEventListener('add', () => {
           this.apiList.load();
        });

        this.apiList.addEventListener('edit', this.edit.bind(this));
        this.apiList.addEventListener('delete', this.delete.bind(this));
        this.apiList.addEventListener('api_list', this.processApiList.bind(this));
        this.apiForm.addEventListener('reset', this.normalMode.bind(this));
        document.getElementById('add2-tab').addEventListener('click', this.add2.bind(this));

    }

    add2() {
        this.apiForm.reset();
        this.normalMode();
        this.showAddTab();
    }

    processApiList(result) {
        this.apiForm.setEngines(result.engines);
    }

    static editMode() {
        document.getElementById('add-edit-title').innerText = "Edit API + Endpoint";
        document.getElementById('fill-in').innerText = "Update any of the following parameters:";
        document.getElementById('add-tab').innerText="Edit";
        document.getElementById('add2-tab').classList.remove("hidden");
    }

    normalMode() {
        document.getElementById('add-edit-title').innerText = "Add API";
        document.getElementById('fill-in').innerText = "Please fill in the following parameters:";
        document.getElementById('add-tab').innerText="Add";
        document.getElementById('add2-tab').classList.add("hidden");
        const switcher = document.querySelector('div.switch');
        const radio = document.getElementById('switch-api-endpoint');
        radio.checked = false;
        switcher.classList.remove('hidden');
        this.apiForm.hideForm();
    }

    delete(dbid) {
        this.apiList.showLoading();
        fetchJson('/api/' + dbid, {}, "delete").then(
            result => {
                this.apiList.load();
            },
            msg => {
                alert(msg);
            }
        );
    }

    showAddTab() {
        document.getElementById('list').classList.remove('active');
        document.getElementById('list').classList.remove('show');
        document.getElementById('list-tab').classList.remove('active');
        document.getElementById('add').classList.add('active');
        document.getElementById('add').classList.add('show');
        document.getElementById('add-tab').classList.add('active');
    }

    edit(dbid) {
        fetchJson('/api/' + dbid, {}, "get").then(
            result => {
                this.apiForm.reset();
                const engine = result.engine;
                const database = result.database;
                const obj = result['object'];

                const switcher = document.querySelector('div.switch');
                const radio = document.getElementById('switch-api-endpoint');
                radio.checked = true;
                switcher.classList.add('hidden');
                document.querySelector('input[name=eid]').value = engine[0];
                document.querySelector('input[name=dbid]').value = database[0];
                document.querySelector('input[name=oid]').value = obj[0];

                const endpointParameters = new Map();
                const apiParameters = new Map();
                let connectionPropertiesStr = engine[4];
                connectionPropertiesStr = connectionPropertiesStr.replace(/^[a-zA-Z0-9]+:/, "");
                const connectionProperties = ApiForm.parseConnectionProperties(connectionPropertiesStr);
                apiParameters.set('connection_properties', connectionProperties);
                apiParameters.set('name', engine[1]);
                endpointParameters.set('name', database[2]);
                endpointParameters.set('password_field', ApiForm.parseConnectionProperties(database[4]));
                endpointParameters.set('url', obj[1]);
                endpointParameters.set('result_key', obj[2]);
                this.apiForm.setParameters(apiParameters, endpointParameters);
                this.apiForm.showApiEndpoint();
                this.apiForm.showForm();
                Master.editMode();
                this.showAddTab();
            },
            msg => {
                alert(msg);
            }
        );
    }
}

window.addEventListener('load', () => {
   const master = new Master();
});

