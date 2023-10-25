xquery version "1.0-ml";

module namespace resource = "http://marklogic.com/rest-api/resource/multipleItems";

declare function get($context as map:map,$params  as map:map) as document-node()*
{
  xdmp:log(("GET multipleItems called", $context, $params)), (
    document {<first>doc1</first>},
    document {<second>doc2</second>}
  )
};