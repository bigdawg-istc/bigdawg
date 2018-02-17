/**
 * A few elements we may need to later manipulate
 */
const button = document.querySelector('#query ~ button');
const queryTextarea = document.querySelector('#query');
const resultTextarea = document.querySelector('#result');
const resultDiv = document.querySelector('.result');
const resultTableDiv = document.querySelector('.result .table');
const resultTextareaDiv = document.querySelector('.result .textarea');
const loadingDiv = document.querySelector('.loading');
const exportCSVFilenameDiv = document.querySelector('.export-csv-filename');
const exportForm = exportCSVFilenameDiv.querySelector('.form-inline');
const queryErrorEle = document.getElementById('query-error')
exportForm.addEventListener('submit', exportCsvFilenameSubmit);

let curTable = [];

// Set the event listener for the button's click
button.addEventListener('click', query);

/**
 * The following functions show/hide the loading spinner
 */
function startLoading() {
    button.disabled = true;
    loadingDiv.style.display = 'block';
}

function stopLoading() {
    button.disabled = false;
    loadingDiv.style.display = 'none';
}

function showError(msg) {
    showMsg(queryErrorEle, msg);
}

function hideError() {
    hide(queryErrorEle)
}

/**
 * Clears our error and success results
 */
function clearResults() {
    resultDiv.style.display='none';
    resultTableDiv.style.display='none';
    resultTableDiv.innerHTML='';
    exportCSVFilenameDiv.style.display='none';
    hideError();
}

/**
 * Shows the success response
 */
function displayResultTable(html) {
    resultTableDiv.innerHTML = html;
    resultTableDiv.style.display='block';
    resultDiv.style.display='block';
    const exportCSVspan = document.querySelector('.export-csv');
    exportCSVspan.addEventListener('click', showDownloadCsv)
}

function showDownloadCsv() {
    exportCSVFilenameDiv.style.display ='block';
    const exportCSVspan = document.querySelector('.export-csv');
    exportCSVspan.style.display = 'none';
}

function exportCsvFilenameSubmit(e) {
    e.preventDefault();

    const filename = this.querySelector('#csv-filename').value;
    if (!filename) {
        alert("Please input a filename.");
        return;
    }

    const csvContents = createCSV();
    const link = document.createElement('a');
    link.setAttribute('href', encodeURI(csvContents));
    link.setAttribute('download', filename);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    this.querySelector('#csv-filename').value = '';
    exportCSVFilenameDiv.style.display='none';
    document.querySelector('.export-csv').style.display='inline';
    return false;
}

function createCSV() {
    let csv = 'data:text/csv;charset=utf-8,';
    curTable.forEach(columns => {
        const finalColumns = columns.map(function(item) {
            let doubleQuotes = false;
            if (item.match(/"/)) {
                item = item.replace(/"/, '""');
                doubleQuotes = true;
            }
            if (item.match(/,/)) {
                doubleQuotes = true;
            }
            if (doubleQuotes) {
                return '"' + item + '"';
            }
            return item;
        });

        csv += finalColumns.join(',');
        csv += "\r\n";
    });
    return csv;
}

/**
 * Shows an error response in a readonly textarea
 */
function displayTextarea(text) {
    resultTextareaDiv.style.display='block';
    resultTextarea.value = text;
    resultDiv.style.display='block';
}

/**
 * Runs the query in the background, showing a loading spinner
 *
 * Should prevent multiple queries from being run by disabling the run button
 */
function query() {
    clearResults();
    startLoading();
    const val = queryTextarea.value;
    if (!val) {
      alert("No query passed in");
      return;
    }

    fetch('/run_query', {
        method: 'post',
        headers: {
            "Content-Type": "application/json"
        },
        body: val
    }).then(response => {
        response.text().then(result => {
            stopLoading();
            if (result.match(/\t/)) {
                const html = resultTable(result);
                displayResultTable(html);
                return;
            }
            showError(result);
        }, response => {
            stopLoading();
            showError("Unknown error - see Javascript console for more details");
            console.log('error - could not parse response to text', response);
        });
    }, response => {
        stopLoading();
        showError("Unknown response - are we offline? - see Javascript console for more details");
        console.log(response);
    });
}

/**
 * Creates an html table of the results
 */
function resultTable(result) {
    const lines = result.split(/\r?\n/);
    let table = '<table class="table table-striped"><caption>Results</caption>';
    const lineColumns = [];
    let maxLength = 0;
    /** find the longest column */
    lines.forEach(line => {
        const columns = line.split(/\t/);
        lineColumns.push(columns);
        if (columns.length > maxLength) {
            maxLength = columns.length;
        }
    });
    curTable = lineColumns;
    lineColumns.forEach(columns => {
        table += '<tr>';
        columns.forEach(column => {
            table += '<td>' + escapeSpecial(column) + '</td>';
        });
        /** if column too short, extend so that things look "right" */
        if (columns.length < maxLength) {
            for(let i = columns.length; i < maxLength; i++) {
                table += '<td></td>';
            }
        }
        table += '</tr>';
    });
    table += '</table>';
    table += '<span class="export-csv">Export CSV</span>';
    return table;
}