---
layout: default
title: QueryRowsMarkLogic
parent: Connector Components
nav_order: 10
---

The `QueryRowsMarkLogic` processor executes a 
[serialized Optic query](https://docs.marklogic.com/guide/app-dev/OpticAPI#id_11208) and writes the returned rows to 
a FlowFile. The processor requires a MarkLogic user with the `rest-reader` privilege. 

The primary use case for this processor is when data should be retrieved as rows as opposed to documents, for which 
[`QueryMarkLogic`](query.md) is a more suitable choice.

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Documentation".
