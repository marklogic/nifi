const flowApi = require('/data-hub/public/flow/flow-api.sjs');

function main(content, options) {
  const inputDocument = content.value;

  const instance = inputDocument.toObject();
  const headers = {};
  const triples = [];
  const outputFormat = 'json';
  const envelope = flowApi.makeEnvelope(instance, headers, triples, outputFormat);

  return {
    uri: content.uri,
    value: envelope,
    context: content.context
  };
}

module.exports = {
  main
};
