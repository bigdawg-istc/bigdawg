const databaseEle = document.getElementById('database');
const objectEle = document.getElementById('object');
const desiredFormatEle = document.getElementById('desired-format');
const desiredFormatWrapEle = document.getElementById('desired-format-wrap');
const fileEle = document.querySelector('input[type=file]');
const headerEle = document.querySelector('input[type=checkbox]');
const buttonEle = document.querySelector('button.btn-primary');
const formEle = document.querySelector('form.upload-form');
const loadingEle = document.querySelector('div.loading');
const uploadErrorEle = document.getElementById('upload-error');
const uploadSuccessEle = document.getElementById('upload-success');
const filenameEle = document.getElementById('filename');

formEle.addEventListener('submit', (e) => { e.preventDefault(); return false; });

let currentCsv = null;
databaseEle.addEventListener('change', populateObjects);
objectEle.addEventListener('change', setDesiredFormat);
fileEle.addEventListener('change', checkFile);
buttonEle.addEventListener('click', uploadCsv);
headerEle.addEventListener('change', verifyHeader);

let firstFetch = false;

function uploadCsv() {
    hideError();
    hideSuccess();
    startLoading();
    const csv = currentCsv;
    const oid = parseInt(getSelected(objectEle));
    if (null === oid) {
        stopLoading();
        showError("No object selected.");
        return;
    }

    if (currentCsv === null) {
        stopLoading();
        let msg = "CSV not yet uploaded";
        if (fileEle.files && fileEle.files[0]) {
            msg = "CSV not in proper format, please fix first";
        }
        showError(msg);
        return;
    }

    if (headerEle.checked) {
        csv.shift();
    }
    const uploadData = { csv: csv,
                         oid: oid };
    const upload = JSON.stringify(uploadData);
    doUpload(upload);
}

function startLoading() {
    buttonEle.disabled = true;
    loadingEle.style.visibility = 'visible';
}

function stopLoading() {
    buttonEle.disabled = false;
    loadingEle.style.visibility = 'hidden';
}

function showError(msg) {
    showMsg(uploadErrorEle, msg);
}

function hideError() {
    hide(uploadErrorEle)
}

function hideSuccess() {
    hide(uploadSuccessEle);
}

function doUpload(val) {
    fetch('/import_csv', {
        method: 'post',
        headers: {
            "Content-Type": "application/json"
        },
        body: val
    }).then(response => {
        response.text().then(result => {
            stopLoading();

            try {
                result = JSON.parse(result);
            }
            catch (e) {
                showError("Error parsing response, please see console for details.");
                console.log('response', response, result, e);
                return;
            }
            if (!result) {
                showError("Could not parse response, please see console for details.");
                console.log('response', response, result);
                return;
            }

            if (result.success) {
                showMsg(uploadSuccessEle, 'Success');
                return;
            }

            showError('Error: ' + result.error);
        }, result => {
            showError("Could not parse response - see console for more details");
            console.log('error - could not parse response to text', result, response);
            stopLoading();
        });
    }, response => {
        console.log(response);
        showError("Bad response - see console for more details");
        stopLoading();
    });
}

function setFilename(name) {
    filenameEle.innerText = name;
    filenameEle.style.display = 'inline-block';
}

function resetFilename() {
    filenameEle.innerText = '';
    filenameEle.style.display = 'none';
}

function checkFile() {
    if (!fileEle.files || !fileEle.files[0]) {
        return;
    }

    const file = fileEle.files[0];
    if (file.name) {
        setFilename(file.name);
    }
    console.log(file, file.name);
    readFile(file);
}

function readFile(file) {
    const reader = new FileReader();
    reader.addEventListener('load', processCsv);
    reader.readAsText(file);
}

function verifyHeader() {
    if (!headerEle.checked) {
        return;
    }

    if (null === currentCsv) {
        return;
    }

    if (currentCsv.length === 0) {
        showError("CSV is empty");
        return;
    }

    const objectInfo = getObjectInfo();
    if (!objectInfo) {
        return;
    }
    const { fields, fieldsSplit, fieldsCount } = objectInfo;
    const csvLine = currentCsv[0];

    let header = true;
    csvLine.forEach((member, idx) => {
        if (fieldsSplit[idx] !== member) {
            header = false;
        }
    });

    if (!header) {
        showError("CSV headings don't match with the list of fields above");
        headerEle.checked = false;
        return false;
    }

    return true;
}

function getObjectInfo() {
    const objectSelected = parseInt(getSelected(objectEle));
    if (null === objectSelected) {
        showError("Error: no selected object");
        return;
    }
    const object = objectsById[objectSelected];
    if (!object) {
        showError("No object found for oid: " + objectSelected);
        return;
    }
    const fields = object.fields;
    const fieldsSplit = fields.split(',');
    const fieldsCount = fieldsSplit.length;
    return { fields, fieldsSplit, fieldsCount };
}

function processCsv(e) {
    hideError();
    hideSuccess();
    currentCsv = null;
    buttonEle.style.visibility = 'hidden';

    const data = this.result;
    const objectSelected = parseInt(getSelected(objectEle));

    if (null === objectSelected) {
        showError("Error: no selected object");
        fileEle.value = null;
        return;
    }
    const object = objectsById[objectSelected];
    if (!object) {
        showError("No object found for oid: " + objectSelected);
        fileEle.value = null;
        return;
    }
    const objectInfo = getObjectInfo();
    if (!objectInfo) {
        return;
    }
    const { fields, fieldsSplit, fieldsCount } = objectInfo;
    const csvLines = data.split(/\r?\n/);
    const csvLinesLen = csvLines.length;
    const csvFinal = [];
    const errors = [];
    csvLines.forEach((line, idx) => {
        if (!line && idx === (csvLinesLen - 1)) {
            // File is ending on a new line, so skip.
            return;
        }
        const parsedLine = parseCsvLine(line);
        if (parsedLine.length !== fieldsCount) {
            errors.push("Line " + (idx + 1) + " has wrong number of fields: " + parsedLine.length + " while expecting " + fieldsCount);
            return;
        }
        if (idx === 0) {
            // Check if header line
            let header = true;
            parsedLine.forEach((member, idx) => {
                console.log(idx, fieldsSplit[idx], member);
                if (fieldsSplit[idx] !== member) {
                    console.log('header', header);
                    header = false;
                }
            });
            if (header) {
                headerEle.checked = true;
                csvFinal.push(parsedLine);
                return;
            }
        }

        // convert types
        fieldsSplit.forEach((item, idx) => {
            if (datatypesByTable[object.name] &&
                    datatypesByTable[object.name][fieldsSplit[idx]]) {
                const dataType = datatypesByTable[object.name][fieldsSplit[idx]];
                if (dataType === 'integer') {
                    parsedLine[idx] = parseInt(parsedLine[idx]);
                }
                if (dataType === 'double precision') {
                    parsedLine[idx] = parseFloat(parsedLine[idx]);
                }
            }
        });

        csvFinal.push(parsedLine);
    });
    if (errors.length) {
        if (errors.length > 10) {
            errors.splice(10);
            errors.push("...");
        }
        showError(errors.join("\n"));
        return;
    }
    currentCsv = csvFinal;
    buttonEle.style.visibility = 'visible';
}

function parseCsvLine(line) {
    let state = 'start';
    const len = line.length;

    const parsed = [];
    let currentPart = "";

    for (let i = 0; i < len; i++) {
        switch (line[i])
        {
            case ',':
                if (state !== 'quote') {
                    parsed.push(currentPart);
                    currentPart = "";
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

function resetDesiredFormat() {
    desiredFormatEle.innerHTML = '';
    desiredFormatWrapEle.style.visibility = 'hidden';
    formEle.style.visibility = 'hidden';
    buttonEle.style.visibility = 'hidden';
    fileEle.value = null;
    headerEle.checked = false;
    resetFilename();
    hideError();
    hideSuccess();
}

function setDesiredFormat() {
    const selectedObject = getSelected(objectEle);
    resetDesiredFormat();
    if (null === selectedObject) {
        return;
    }

    const oid = parseInt(selectedObject);
    if (objectsById[oid]) {
        desiredFormatEle.innerHTML = escapeSpecial(objectsById[oid].fields);
        desiredFormatWrapEle.style.visibility = 'visible';
        formEle.style.visibility = 'visible';
    }
}

function getSelected(ele) {
    if (ele.selectedIndex >= 0) {
        const options = ele.getElementsByTagName('option');
        if (options[ele.selectedIndex]) {
            return options[ele.selectedIndex].value || null;
        }
    }
    return null;
}

function populateObjects() {
    const defaultHtml = '<option>Select Database First</option>';
    const selectedDatabase = parseInt(getSelected(databaseEle));
    if (null === selectedDatabase) {
        objectEle.innerHTML = defaultHtml;
        resetDesiredFormat();
        return;
    }

    let optionHtml = "";
    const selectedDatabaseNum = parseInt(selectedDatabase);
    objects.forEach((item) => {
        if (item.physical_db === selectedDatabaseNum) {
            optionHtml += '<option value="' + item.oid + '">' + escapeSpecial(item.name) + '</option>\n';
        }
    });
    objectEle.innerHTML = optionHtml || defaultHtml;
    fileEle.value = null;
    headerEle.checked = false;
    setDesiredFormat();
}
populateObjects();