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

function fetchJson(url, body) {
    return new Promise((resolve, reject) => {
        fetch(url, {
            method: 'post',
            headers: {
                "Content-Type": "application/json"
            },
            body: body
        }).then(response => {
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
