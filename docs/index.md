---
layout: default
title: Introduction
nav_order: 1
---

The MarkLogic connector for [Apache NiFi](https://nifi.apache.org/) allows for NiFi to be used to easily import and 
export data to and from MarkLogic, along with running batch processes on data already in MarkLogic. 

The connector has the following system requirements:

* Apache NiFi 2.0 or higher; the connector may run successfully on earlier versions of NiFi but it is only 
  actively tested on NiFi 2.3.0 and higher.
* MarkLogic 10 or higher, though processors that do not depend on any functionality in MarkLogic 10 or later will 
  run fine on MarkLogic 9.

Please see the [Getting Started guide](getting-started.md) to begin using the connector.

For any issues, you can either [report an issue](https://github.com/marklogic/nifi/issues) or contact MarkLogic 
Support if you are a MarkLogic customer.
