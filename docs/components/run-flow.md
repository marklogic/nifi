---
layout: default
title: RunFlowMarkLogic
parent: Connector Components
nav_order: 11
---

The `RunFlowMarkLogic` processor supports running a 
[MarkLogic Data Hub flow](https://docs.marklogic.com/datahub/6.2/flows/about-flows.html) and returning the result as 
a new FlowFile. The flow is expected to run non-ingestion steps, where data has already been ingested into MarkLogic 
and needs to be reprocessed via a Data Hub flow. 

In order to support running a Data Hub flow, the connector must depend on a particular version of the Data Hub Java 
API. As of the 6.1.1 release of the Data Hub Java API, there is not a guarantee that the Data Hub Java API will work 
with any release of Data Hub other than its own. It is therefore recommended to ensure that the version of Data Hub 
you have installed in MarkLogic is the same as that of the MarkLogic NiFi connector release that you are using. 
The NiFi 2.0.0 Marklogic connector release contains the Data Hub 6.2.0 release.

The table below lists the MarkLogic NiFi connector releases and the associated Data Hub Java API version:

| Connector version | Data Hub Java API version |
|-------------------|---------------------------|
| 2.0.0             | 6.2.0                     |
| 1.27.0            | 6.1.1                     |
| 1.24.2            | 5.8.1                     |
| 1.16.3.1          | 5.7.2                     |
| 1.15.3.2          | 5.4.4                     |
| 1.9.1.7           | 5.4.2                     |

Please see the [Data Hub documentation for roles](https://docs.marklogic.com/datahub/6.2/security/users-and-roles.html) for which roles a MarkLogic user will need in order to run a flow.

For full details on each property and relationship for this processor, add an instance of it to the NiFi canvas,
right-click on it, and select "View Documentation".
