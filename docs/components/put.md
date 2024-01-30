---
layout: default
title: PutMarkLogic
parent: Connector Components
nav_order: 7
---

The `PutMarkLogic` processor writes batches of FlowFiles as documents to a MarkLogic server using the 
[MarkLogic Data Movement SDK](https://docs.marklogic.com/guide/java/data-movement). The processor requires a MarkLogic 
user with the 'rest-writer' privilege.

The processor uses NiFi's support for running when no upstream FlowFiles are available. This ensures that partial 
batches will be written to MarkLogic without having to wait for an upstream FlowFile to appear. The NiFi 
"Yield Duration" setting can be used to control how often this processor will check for new FlowFiles and flush 
partial batches.

An example of this processor in action can be found in the [Getting Started guide](../getting-started.md).

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
