---
layout: default
title: DeleteMarkLogic
parent: Connector Components
nav_order: 4
---

The `DeleteTransformMarkLogic` processor is similar to [`QueryMarkLogic`](query.md) but is instead used to delete
documents that match a query. A new FlowFile is created for each deleted document. The processor requires a MarkLogic 
user with the `rest-reader` privilege to read documents and the `rest-writer` privilege to delete documents.

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
