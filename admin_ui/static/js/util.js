/**
 * need to add characters if necessary for valid html
 */
function escapeSpecial(text) {
    text = text.replace(/&/,'&amp;');
    text = text.replace(/</,'&lt;');
    text = text.replace(/>/,'&gt;');
    return text;
}

function showMsg(ele, msg) {
    ele.style.display = 'block';
    ele.innerText = msg;
}

function hide(ele) {
    ele.style.display = 'none';
    ele.innerText = '';
}

function fetchJson(url, body, method = "post") {
    return new Promise((resolve, reject) => {
        const options = {
            method: method,
            headers: {
                "Content-Type": "application/json"
            }
        };
        if (method.toUpperCase() === "POST") {
            options.body = body;
        }
        fetch(url, options).then(response => {
            response.text().then(result => {
                try {
                    result = JSON.parse(result);
                }
                catch (e) {
                    console.log('response', response, result, e);
                    reject('Error parsing response, please see console for details.');
                    return;
                }
                if (!result) {
                    console.log('response', response, result);
                    reject('Could not parse response, please see console for details.');
                    return;
                }

                if (result.success) {
                    resolve(result);
                    return;
                }

                reject('Error: ' + result.error);
            }, result => {
                console.log('error - could not parse response to text', result, response);
                reject('Could not parse response - see console for more details');
            });
        }, response => {
            console.log(response);
            reject('Bad response - see console for more details');
        });
    });
}

class Evented {
    fireEvent(name, info) {
        if (this.events && this.events[name]) {
            this.events[name].forEach((callback) => {
                callback(info);
            });
        }
    }

    addEventListener(event, callbackFn) {
        if (!this.events) {
            this.events = {};
        }
        if (!this.events[event]) {
            this.events[event] = [];
        }

        this.events[event].push(callbackFn);
    }
}
