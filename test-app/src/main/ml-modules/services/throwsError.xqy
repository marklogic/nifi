xquery version "1.0-ml";

module namespace resource = "http://marklogic.com/rest-api/resource/throwsError";

declare function get(
        $context as map:map,
        $params as map:map
) as document-node()*
{
    fn:error(xs:QName("GetErrorQName"), "GetError Description", (500, "GetError message"))
};
