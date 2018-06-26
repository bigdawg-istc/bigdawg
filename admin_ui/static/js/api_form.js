class Main {
    constructor() {
        this.api = new FormType('api');
        this.endpoint = new FormType('endpoint');
        this.auth = new Auth();
        this.advanced = new Advanced();
        this.addEventListeners();
        this.toggle();
        this.errorElement = document.getElementById('error');
        this.successElement = document.getElementById('success');
        this.api.reset();
        this.advanced.reset();
        this.endpoint.reset();
        this.auth.reset();
    }

    addEventListeners() {
        document.querySelectorAll('input[name=type]').forEach(
            (element) => {
                element.addEventListener('change', this.toggle.bind(this));
            });

        document.querySelector('form').addEventListener('submit', this.submit.bind(this));
        document.getElementById('name').addEventListener('keyup', this.checkApiName.bind(this));
    }

    toggle() {
        document.querySelectorAll('input[name=type]').forEach(
            (element) => {
                if (element.checked) {
                    document.querySelector('form').classList.remove("hidden");
                    this.hideSuccess();
                    switch(element.value) {
                        case "api":
                            this.showApi();
                            break;
                        case "endpoint":
                            this.api.hide();
                            this.auth.hide();
                            this.endpoint.show();
                            break;
                        case "api+endpoint":
                            this.showApiEndpoint();
                            break;
                        default:
                            throw "Unknown element";
                    }
                    Main.showButton();
                }
            }
        );
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

    getFormData() {
        const api = this.api.getVisibleFormData();
        const endpoint = this.endpoint.getVisibleFormData();
        if (api.size > 0) {
            this.advanced.getFormData(api);
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
            data['api']['connection_properties'] = 'REST:' + Main.convertConnectionProperties(api.get('connection_properties'));
        }

        endpoint.forEach((value, key) => {
            if (!data['endpoint']) {
                data['endpoint'] = {};
            }
            data['endpoint'][key] = value;
        });

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
                    throw "Unknown port type: " + aElement.protocoll;
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
        document.querySelector('.loading').style.display='none';
    }

    static showLoading() {
        document.querySelector('.loading').style.display='block';
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
            fetch('/get_engine_by_name', {
                method: 'post',
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({name: name})
            }).then(response => {
                response.text().then(result => {
                    try {
                        result = JSON.parse(result);
                    }
                    catch (e) {
                        console.log('response', response, result, e);
                        return;
                    }
                    if (!result) {
                        console.log('response', response, result);
                        return;
                    }

                    if (result.success) {
                        if (this.currentNameCheck === name) {
                            this.showApiNameDuplication();
                        }
                    }
                }, result => {
                    console.log('error - could not parse response to text', result, response);
                });
            }, response => {
                console.log(response);
            });
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

        Main.showLoading();
        Main.disableForm();
        this.hideError();
        const formData = this.getFormData();
        fetch('/api_form', {
            method: 'post',
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(formData)
        }).then(response => {
            response.text().then(result => {
                Main.hideLoading();
                Main.enableForm();
                try {
                    result = JSON.parse(result);
                }
                catch (e) {
                    this.showError("Error parsing response, please see console for details.");
                    console.log('response', response, result, e);
                    return;
                }
                if (!result) {
                    this.showError("Could not parse response, please see console for details.");
                    console.log('response', response, result);
                    return;
                }

                if (result.success) {
                    this.api.hide();
                    this.auth.hide();
                    this.endpoint.hide();
                    this.advanced.hide();
                    Main.hideButton();
                    Main.uncheckType();
                    this.showSuccess('Success');
                    this.api.reset();
                    this.auth.reset();
                    this.endpoint.reset();
                    this.advanced.reset();
                    return;
                }

                this.showError('Error: ' + result.error);
            }, result => {
                Main.hideLoading();
                Main.enableForm();
                this.showError("Could not parse response - see console for more details");
                console.log('error - could not parse response to text', result, response);
            });
        }, response => {
            Main.hideLoading();
            Main.enableForm();
            console.log(response);
            this.showError("Bad response - see console for more details");
        });
        return false;
    }

    static setInputPair(pairs, input, key, value) {
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
                    if (input.dataset && input.dataset.name) {
                        name = input.dataset.name;
                    }
                    Main.setInputPair(pairs, input, name, input.value);
                }
            }
        });
        return pairs;
    }

    static resetSubElements(element) {
        element.querySelectorAll('input[type=text]').forEach(item => {
            item.value = "";
        });
        element.querySelectorAll('select').forEach(item => {
            item.selectedIndex = -1;
        });
        const radio = element.querySelector('input[type=radio]');
        if (radio) {
            radio.checked = true;
        }
        element.querySelectorAll('input[type=checkbox]').forEach(item => {
            item.checked = false;
        });
    }
}



class Advanced {
    constructor() {
        this.queryParams = new KeyValueParameters('query_params', 'extra-query', 'Extra Query Parameters');
        this.addEventListeners();
    }

    reset() {
        document.querySelectorAll('.advanced').forEach((item) => {
            Main.resetSubElements(item);
        });
        this.queryParams.reset();
    }

    addEventListeners() {
        document.getElementById('advanced-link').addEventListener('click', this.toggleAdvanced.bind(this));
    }

    show() {
        document.querySelectorAll('.advanced').forEach((item) => {
            item.classList.remove("hidden");
        });
        document.getElementById('advanced-link').innerText = "Advanced <<";
        this.queryParams.toggleParameters();
    }

    hide() {
        document.querySelectorAll('.advanced').forEach((item) => {
            item.classList.add("hidden");
        });
        document.getElementById('advanced-link').innerText = "Advanced >>";
        this.queryParams.hideParameters();
    }

    toggleAdvanced() {
        if (document.getElementById('advanced-link').innerText === "Advanced >>") {
            this.show();
            return;
        }
        this.hide();
    }

    getFormData(pairs = new Map()) {
        document.querySelectorAll(`.advanced`).forEach((element) => {
            if (element.querySelector('.extra-query')) {
                return;
            }
            Main.collectFormData(element, pairs);
        });

        const queryParams = this.queryParams.getFormData();
        if (queryParams) {

            pairs.set('query_params',queryParams);
        }
        return pairs;
    }
}

class FormType {
    constructor(className) {
        this.className = className;
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
            Main.resetSubElements(element);
        });
    }

    getVisibleFormData(pairs = new Map()) {
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            if (element.classList.contains('hidden')) {
                return;
            }

            Main.collectFormData(element, pairs);
            element.querySelectorAll('select').forEach((select) => {
                if (select.selectedIndex > -1) {
                    const selectedValue = select.selectedOptions[select.selectedIndex].value;
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
        document.querySelector(`.${this.className}`).querySelector('.glyphicon-plus-sign').addEventListener('click', this.addPair.bind(this));
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
            checkboxElement.parentElement.querySelectorAll('input[type=text]').forEach((input) => {
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
               const valueKey = key.replace(/_key_/, 'value');
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
            html += ' <span class="glyphicon glyphicon-minus-sign"></span>';
        }
        const divElement = document.createElement('div');
        divElement.classList.add('form-group');
        divElement.classList.add(this.className);
        divElement.innerHTML = html;
        document.querySelectorAll(`.${this.className}`).forEach((element) => {
            const plus = element.querySelector('.glyphicon-plus-sign');
            if (plus) {
                element.removeChild(plus);
            }
        });
        document.querySelector(`.${this.className}`).parentNode.appendChild(divElement);
        divElement.querySelector('.glyphicon-minus-sign').addEventListener('click', this.removePair.bind(this));
        divElement.querySelector('.glyphicon-plus-sign').addEventListener('click', this.addPair.bind(this));
    }

    reset() {
        let minus = null;
        while (minus = document.querySelector(`.${this.className} .glyphicon-minus-sign`)) {
            this.removePairByElement(minus);
        }
        Main.resetSubElements(document.querySelector(`.${this.className}`));
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
        span.classList.add('glyphicon');
        span.classList.add('glyphicon-plus-sign');
        span.addEventListener('click', this.addPair.bind(this));
        if (potentialMinusElement.classList && potentialMinusElement.classList.contains('glyphicon-minus-sign')) {
            if (!potentialMinusElement.parentNode.querySelector('.glyphicon-plus-sign')) {
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
            Main.resetSubElements(element);
        });
        this.oauth2AuthResponseValidate.reset();
    }

    addEventListeners() {
        document.querySelectorAll('input[name=auth_type]').forEach((item) => {
           item.addEventListener('change', this.toggleAuth.bind(this));
        });
        document.getElementById('oauth2_auth_response_validate').addEventListener('change', Auth.toggleOAuth2ResponseValidation);
        document.querySelectorAll('input[name=oauth2_auth_method]').forEach((element) => {
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
            element.childNodes.forEach((item) => {
                if (!item.classList || item.classList.contains('hidden')) {
                   return;
                }

                if (item.id === 'auth-response-validate') {
                    const authParameters = this.oauth2AuthResponseValidate.getFormData();
                    if (authParameters) {
                        pairs.set('auth_response_validate', authParameters);
                    }
                    return;
                }

                Main.collectFormData(element, pairs);
            });
        });
    }


    static toggleOAuth2Method() {
        document.querySelectorAll('input[name=oauth2_auth_method]').forEach((element) => {
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

window.addEventListener('load', () => {
   const main = new Main();
});

