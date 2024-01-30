---
layout: default
title: QueryMarkLogic
parent: Connector Components
nav_order: 9
---

The `QueryMarkLogic` processor supports querying for large numbers of documents and/or metadata and asynchronously 
creating FlowFiles as batches of documents and/or metadata are retrieved from MarkLogic. The processor leverages the 
[MarkLogic Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement) and requires that the MarkLogic 
user have the `rest-reader` privilege.

The primary use case for this processor is for retrieving documents from MarkLogic for processing by other NiFi 
processors. URIs and document metadata may also be retrieved as well. 

An example of this processor in action can be found in the [Getting Started guide](../getting-started.md). 

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
