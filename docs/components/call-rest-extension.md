---
layout: default
title: CallRestExtensionMarkLogic
parent: Connector Components
nav_order: 3
---

The `CallRestExtensionMarkLogic` processor supports calling a 
[MarkLogic REST extension](https://docs.marklogic.com/guide/rest-dev/extensions#id_14646) with results being sent to one 
or more new FlowFiles. The required privileges for the MarkLogic user depend on the 
[HTTP method that is called on the REST extension](https://docs.marklogic.com/REST/client/service-extension).

This processor is intended to replace [`ExtensionCallMarkLogic`](extension-call.md), which was deprecated in the
1.16.3.1 release. 

The primary use case for this controller is in executing custom code in a more abstracted and secure way than what 
is supported by [`ExecuteScriptMarkLogic`](execute-script.md). 

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
