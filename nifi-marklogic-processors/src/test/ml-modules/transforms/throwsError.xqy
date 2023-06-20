xquery version "1.0-ml";

module namespace transform = "http://marklogic.com/rest-api/transform/throwsError";

declare function transform(
  $context as map:map,
  $params as map:map,
  $content as document-node()
  ) as document-node()
{
  let $throw-error-for-uri := map:get($params, "throwErrorForUri")
  return
    if ($throw-error-for-uri = map:get($context, "uri") or fn:not($throw-error-for-uri)) then
      fn:error((), "RESTAPI-SRVEXERR", (400, "Intentional error from throwsError transform"))
    else
      $content
};
