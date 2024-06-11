---
layout: default
title: PutMarkLogicRecord
parent: Connector Components
nav_order: 8
---

The `PutMarkLogicRecord` processor differs from the [`PutMarkLogic`](put.md) processor in that instead of inserting 
a document per FlowFile, it breaks a FlowFile into batches of documents that are inserted into MarkLogic. The 
processor depends on a NiFi `RecordReader` for controlling how multiple records are extracted from a single 
FlowFile. The processor also depends on a NiFi `RecordWriter` for controlling how records are serialized before they 
are written as documents to MarkLogic.

The processor requires a MarkLogic user with the `rest-writer` privilege.

The primary use case for this processor is when an incoming FlowFile should result in multiple documents being 
written to MarkLogic as opposed to just one document being written. A FlowFile representing a CSV file is a common 
example of when this processor would be used. 

An example of this processor in action can be found in the [Getting Started guide](../getting-started.md).

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".

## Restart support

Please see the [`PutMarkLogic` documentation](put.md) for information on the restart support added in the 
1.24.2 release of the connector.
