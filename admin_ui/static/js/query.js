class Query {
    constructor() {
        this.button = document.querySelector('#query ~ button');
        this.queryTextarea = document.querySelector('#query');
        this.resultTextarea = document.querySelector('#result');
        this.resultDiv = document.querySelector('.result');
        this.resultTableDiv = document.querySelector('.result .table-div');
        this.resultTextareaDiv = document.querySelector('.result .textarea');
        this.loadingDiv = document.querySelector('.loading');
        this.exportCSVFilenameDiv = document.querySelector('.export-csv-filename');
        this.exportForm = this.exportCSVFilenameDiv.querySelector('.form-inline');
        this.queryErrorEle = document.getElementById('query-error');
        this.addEventListeners();
        this.curTable = [];
    }

    addEventListeners() {
        this.exportForm.addEventListener('submit', this.exportCsvFilenameSubmit.bind(this));
        this.button.addEventListener('click', this.query.bind(this));
    }

    exportCsvFilenameSubmit(e) {
        e.preventDefault();

        const filename = document.querySelector('#csv-filename').value;
        if (!filename) {
            alert("Please input a filename.");
            return;
        }

        const csvContents = this.createCSV();
        const link = document.createElement('a');
        link.setAttribute('href', encodeURI(csvContents));
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        document.querySelector('#csv-filename').value = '';
        this.exportCSVFilenameDiv.style.display='none';
        document.querySelector('.export-csv').style.display='inline';
        return false;
    }

    createCSV() {
        let csv = 'data:text/csv;charset=utf-8,';
        this.curTable.forEach(columns => {
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
     * The following functions show/hide the loading spinner
     */
    startLoading() {
        this.button.disabled = true;
        this.loadingDiv.style.display = 'block';
    }

    stopLoading() {
        this.button.disabled = false;
        this.loadingDiv.style.display = 'none';
    }

    showError(msg) {
        showMsg(this.queryErrorEle, msg);
    }

    hideError() {
        hide(this.queryErrorEle)
    }
    /**
     * Clears our error and success results
     */
    clearResults() {
        this.resultDiv.style.display='none';
        this.resultTableDiv.style.display='none';
        this.resultTableDiv.innerHTML='';
        this.exportCSVFilenameDiv.style.display='none';
        this.hideError();
    }
    displayResultTable(html) {
        this.resultTableDiv.innerHTML = html;
        this.resultTableDiv.style.display='block';
        this.resultDiv.style.display='block';
        const exportCSVspan = document.querySelector('.export-csv');
        exportCSVspan.addEventListener('click', this.showDownloadCsv.bind(this))
    }
    showDownloadCsv() {
        this.exportCSVFilenameDiv.style.display ='block';
        const exportCSVspan = document.querySelector('.export-csv');
        exportCSVspan.style.display = 'none';
    }

    displayTextarea(text) {
        this.resultTextareaDiv.style.display='block';
        this.resultTextarea.value = text;
        this.resultDiv.style.display='block';
    }
    query() {
        this.clearResults();
        this.startLoading();
        const val = this.queryTextarea.value;
        if (!val) {
          alert("No query passed in");
          this.stopLoading();
          return;
        }

        fetchJson('/run_query', val, 'post').then(result => {
            this.stopLoading();
            const html = this.resultTable(result.content);
            this.displayResultTable(html);
        }, reject => {
            this.stopLoading();
            if (reject.match(/^Error: /)) {
                this.showError(reject.replace(/^Error: /, ''));
                return;
            }
            this.showError("Unknown error - see Javascript console for more details");
        });
    }

    resultTable(result) {
        const lines = result.replace(/\r?\n$/,'').split(/\r?\n/);
        let table = '<table class="table table-striped">';
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
        this.curTable = lineColumns;
        lineColumns.forEach(columns => {
            table += '<tr>';
            columns.forEach(column => {
                table += '<td>' + escapeSpecial(column) + '</td>';
            });
            /** if column too short, extend so that things look "right" */
            if (columns.length < maxLength) {
                for (let i = columns.length; i < maxLength; i++) {
                    table += '<td></td>';
                }
            }
            table += '</tr>';
        });
        table += '</table>';
        table += '<span class="export-csv">Export CSV</span>';
        return table;
    }
}

window.addEventListener('load', () => {
    const query = new Query();
});