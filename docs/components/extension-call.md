---
layout: default
title: ExtensionCallMarkLogic
parent: Connector Components
nav_order: 6
---

The `ExtensionCallMarkLogic` processor has been deprecated as of the 1.16.3.1 release. It allows for 
a [MarkLogic REST extension](https://docs.marklogic.com/REST/client/service-extension) to be invoked, but it appends 
the output of the call to the incoming FlowFile as opposed to creating a new FlowFile that contains the output. 
[`CallRestExtensionMarkLogic`](call-rest-extension.md) should be used instead.

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
