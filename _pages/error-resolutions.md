---
layout: inner
title: Error Resolutions
lead_text: ''
permalink: /error-resolutions
---

# Error Resolutions

For more information on troubleshooting issues with the MarkLogic Java Client, see the [Java Application Developer's Guide][java-troubleshooting].

## Common Errors

### FailedRequestException

This error covers multiple issues. The associated server message offers a clue to the root cause.

### UnauthorizedUserException

Message
 : [ProcessorClass] failed! Verify your credentials are correct.

Resolution
 : Check the [Database Client Service][client-service] to verify that your credentials are correct.

### ForbiddenUserException

Message
 : [ProcessorClass] failed! Verify your user has ample privileges.

Resolution
 : Check the MarkLogic server to verify that your user has the required privileges. See the [MarkLogic Security Guide][security-guide].

### ResourceNotFoundException

Message
 : [ProcessorClass] failed due to 'Resource Not Found'! 

Resolution
 : Ensure the [Database Client Service][client-service] is pointing to a REST instance and that referenced Server Transforms or REST Extension is installed on the referenced REST instance. For more information on setting up a REST instance see the [MarkLogic REST Developer's Guide][rest-guide]. 
 : Ensure that your REST extensions and transform parameters are written in camel case format. If your REST extensions and transform parameters must be written in the "ml:" prefix format, upgrade your Data Hub version to 5.7 or later.

## Query Related Errors

Message
 : Local message: failed to apply resource at internal/uris: Bad Request. Server Message: XDMP-DOCROOTTEXT: xdmp:get-request-body("xml") -- Invalid root text 

Resolution
 : Ensure your Query property is valid XML 

Message
 : Local message: failed to apply resource at internal/uris: Bad Request. Server Message: XDMP-DOCROOTTEXT: xdmp:get-request-body("json") -- Invalid root text 

Resolution
 : Ensure your Query property is valid JSON 

Message
 : Local message: failed to apply resource at internal/uris: Bad Request. Server Message: REST-INVALIDPARAM: (err:FOER0000) Invalid parameter: Invalid XML query structure

Resolution
 : Ensure your payload is a valid Structured or Combined Query.

Message
 : Local message: failed to apply resource at internal/uris: Internal Server Error. Server Message: XDMP-NOTQUERY

Resolution
 : Ensure your payload is a valid Structured or Combined Query.

[client-service]: ./nifi-features
[java-troubleshooting]: https://docs.marklogic.com/guide/java/troubleshootings
[security-guide]: https://docs.marklogic.com/guide/security/role
[rest-guide]: https://docs.marklogic.com/guide/rest-dev/service
