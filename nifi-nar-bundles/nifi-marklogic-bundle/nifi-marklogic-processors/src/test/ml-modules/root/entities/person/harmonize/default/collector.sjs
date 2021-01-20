function collect(options) {
	var limit = options != null && options.hasOwnProperty('dhf.limit') ?
		parseInt(options['dhf.limit']) :
		Number.MAX_SAFE_INTEGER;

	return cts.uris(null, 'limit=' + limit, cts.trueQuery());
}

module.exports = {
	collect: collect
};