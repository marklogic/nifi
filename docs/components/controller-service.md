---
layout: default
title: DatabaseClient Service
parent: Connector Components
nav_order: 1
---

The DatabaseClient controller service allows for connection and authentication details to be defined for connecting 
to a MarkLogic REST API server. Each of the processors in the MarkLogic connector depends on an instance of this 
controller service. Thus, before using any of the processors, you will first need to define one or more instances of 
this controller service. 

An instance of the DatabaseClient controller service can be added and made available to processors via the
following steps:

1. Click anywhere on the NiFi canvas so that in the "Operate" panel on the left, a process group is shown as selected.
2. Click on the cog icon in the "Operate" panel.
3. Click on the "Controller Services" tab if it is not already selected.
4. Click on the plus icon on the right side of the "Controller Services" tab.
5. In the "Add Controller Service" dialog that appears, enter "MarkLogic".
6. Select the "DefaultMarkLogicDatabaseClientService" item that appears and click "Add".

A new, disabled controller service named "DefaultMarkLogicDatabaseClientService" will now be listed in the
"Controller Services" tab. The service will need to be edited and then enabled in order for it to be used by any of the
MarkLogic processors. The service can be edited by clicking on the cog icon and can be enabled by clicking on the 
lightning icon.

As an example, for connecting to a MarkLogic REST API app server that uses digest authentication, the following
properties would need to be set:

- "Host" set to the host name of one of the hosts in the MarkLogic cluster.
- "Port" set to the port of the MarkLogic REST API app server you wish to connect to.
- "Security Context Type" set to `DIGEST`.
- "Username" and "Password" set based on the MarkLogic user you wish to authenticate as.

It is recommended to rename the service as well to give it a more self-documenting name. For example, if the port of
your MarkLogic app server is 8004, a more useful name might be "marklogic-8004".

After setting the above properties, the controller service will now be in a valid state and can then be enabled by
clicking on the lightning icon.
