---
layout: default
title: Error Handling
nav_order: 4
---

In general, processors in the MarkLogic connector will capture any error that occurs and include it in a FlowFile 
that is sent to a relationship - normally a relationship named "FAILURE". The FlowFile will have an attribute named 
`markLogicErrorMessage` that captures the error message to assist with debugging the failure. 

The following section describes common errors that can occur with any of the MarkLogic processors. For any errors 
involving MarkLogic, such as issues with a port number, always check the MarkLogic server logs for additional 
information.

| Error type | Message | Resolution |
| --- | --- | --- |
|FailedRequestException| Unauthorized | The authentication details in the DatabaseClient controller service are not correct. |
|ForbiddenUserException| You do not have permission to this method and URL | The authentication details in the DatabaseClient controller service are correct, but the user does not have the required privileges for accessing the MarkLogic URL. |
|MarkLogicIOException| Unexpected end of stream | The MarkLogic app server associated with the Port in the DatabaseClient controller service is not able to receive connections, or may have been stopped. |
|UnknownHostException| nodename nor servname provided, or not known | The host in the DatabaseClient controller service is either invalid or unreachable. |
