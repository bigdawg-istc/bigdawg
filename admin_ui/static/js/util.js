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

