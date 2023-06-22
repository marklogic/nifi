---
layout: default
title: ApplyTransformMarkLogic
parent: Connector Components
nav_order: 2
---

The `ApplyTransformMarkLogic` processor is similar to [`QueryMarkLogic`](query.md) but is instead used to modify 
documents that match a query. The processor depends on a 
[MarkLogic REST transform](https://docs.marklogic.com/guide/rest-dev/transforms) for modifying each document. 

The primary use case for this processor is updating a potentially large number of documents in MarkLogic, where the
updates can all be handled via a REST transform.

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
