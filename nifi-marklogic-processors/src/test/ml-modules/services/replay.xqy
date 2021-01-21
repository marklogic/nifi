xquery version "1.0-ml";

module namespace ext = "http://marklogic.com/rest-api/resource/replay";

declare default function namespace "http://www.w3.org/2005/xpath-functions";


(:
 : Params:
 :  text (string)
 :)
declare
function ext:get(
  $context as map:map,
  $params  as map:map
) as document-node()*
{
  document{ map:get($params, "replay") }
};

(:
 : Params:
 :  text (string)
 :)
declare
function ext:post(
  $context as map:map,
  $params  as map:map,
  $input   as document-node()*
) as document-node()*
{
  $input
};

declare
function ext:put(
  $context as map:map,
  $params  as map:map,
  $input   as document-node()*
) as document-node()?
{
  fn:error((),"RESTAPI-SRVEXERR", 
  (400, "This always errors", 
   "This always errors"))
};

