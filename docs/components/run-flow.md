---
layout: default
title: RunFlowMarkLogic
parent: Connector Components
nav_order: 11
---

The `RunFlowMarkLogic` processor supports running a 
[MarkLogic Data Hub flow](https://docs.marklogic.com/datahub/5.8/flows/about-flows.html) and returning the result as 
a new FlowFile. The flow is expected to run non-ingestion steps, where data has already been ingested into MarkLogic 
and needs to be reprocessed via a Data Hub flow. 

As of the 1.16.3.3 release, this processor uses the Data Hub 5.7.2 Java API to run a flow. This may work with more 
recent versions of Data Hub, but running flows in a Data Hub 5.7.2 installation is recommended. 

Please see the [Data Hub documentation for roles](https://docs.marklogic.com/datahub/5.8/security/users-and-roles.
html) for which roles a MarkLogic user will need in order to run a flow.

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Usage".
