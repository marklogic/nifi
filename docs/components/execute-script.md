---
layout: default
title: ExecuteScriptMarkLogic
parent: Connector Components
nav_order: 5
---

The `ExecuteScriptMarkLogic` processor supports executing server-side code in MarkLogic, written in either 
JavaScript or XQuery. The code can either be defined within the processor or referenced as a module installed within 
MarkLogic. Requires a MarkLogic user with the 
[privileges defined by the MarkLogic 'eval' endpoint](https://docs.marklogic.com/REST/POST/v1/eval).

The primary use case for this processor is executing arbitrary code that is typically not associated with simply 
ingesting or exporting data, as [`PutMarkLogic`](put.md) and [`QueryMarkLogic`](query.md) can usually handle those 
use cases. 

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
