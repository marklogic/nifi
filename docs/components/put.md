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

## Restart support

Starting with release 1.24.2, the `PutMarkLogic` and `PutMarkLogicRecord` processors include a new property named 
"Restart Failed Batcher". When set to a value of `true`, the processor will attempt to restart the underlying 
"batcher" - the component that writes batches of documents to MarkLogic - when it stops due to connectivity issues with 
MarkLogic. 

This support is intended to address a scenario where the processor temporarily cannot connect to MarkLogic, but the 
problem may be resolved in minutes or even seconds. When the batcher experiences a connectivity error, it will stop 
itself but will remain stopped while the processor continues to resolve FlowFiles, with each FlowFile being sent to 
the `FAILURE` relationship. By setting "Restart Failed Batcher" to `true`, the processor will attempt to connect to 
MarkLogic each time it receives a FlowFile. If it still cannot connect, the FlowFile will be routed to the `FAILURE`
relationship. If the processor can connect, then the underlying batcher will be started and will resume its expected
behavior of writing batches of documents to MarkLogic.

