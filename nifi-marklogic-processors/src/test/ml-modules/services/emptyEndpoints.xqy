xquery version "1.0-ml";

module namespace resource = "http://marklogic.com/rest-api/resource/emptyEndpoints";

declare function get(
  $context as map:map,
  $params  as map:map
  ) as document-node()*
{
  xdmp:log(("GET called", $context, $params))
};

declare function put(
  $context as map:map,
  $params  as map:map,
  $input   as document-node()*
  ) as document-node()?
{
  xdmp:log(("PUT called", $input, $context, $params))
};

declare function post(
  $context as map:map,
  $params  as map:map,
  $input   as document-node()*
  ) as document-node()*
{
  xdmp:log(("POST called", $input, $context, $params))
};

declare function delete(
  $context as map:map,
  $params  as map:map
  ) as document-node()?
{
  xdmp:log(("DELETE called", $context, $params))
};
