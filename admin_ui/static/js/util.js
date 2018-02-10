/**
 * need to add characters if necessary for valid html
 */
function escapeSpecial(text) {
    text = text.replace(/&/,'&amp;');
    text = text.replace(/</,'&lt;');
    text = text.replace(/>/,'&gt;');
    return text;
}
