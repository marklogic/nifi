---
layout: inner
title: Data Hub Framework With Smart Mastering
lead_text: ''
permalink: /dhf-with-smart-mastering
---

# Data Hub Framework With Smart Mastering

This recipe steps through using the MarkLogic NiFi Bundle with the Data Hub Framework (DHF) and Smart Mastering.

This cookbook uses the example DHF project in the Smart Mastering version 1.2.2 found [here][smart-mastering-dhf-example].

## Initial Setup

For the purpose of having a NiFi flow that covers the end-to-end experience of using DHF and Smart Mastering, we will *not* be following the steps in the example project README.

Setup the project with:
```bash
./gradlew mlDeploy
./gradlew deployMatchOptions
./gradlew deployMergeOptions
```

Load the second organization dataset. The first organization dataset will be loaded via our NiFi flow.

```bash
./gradlew loadOrganizationSource2
```

## NiFi Template Overview

The template for this cookbook can be downloaded [here][dhf-sm-template].

We'll be loading documents into MarkLogic with the `PutMarklogic`processor and the setting the transform to use a DHF input flow. The output of the `batch_success` relationship will feed an `ExtensionCallMarkLogic` processor that calls DHF REST extension for running a harmonization flow. The `success` relationship of the DHF REST call will feed a second `ExtensionCallMarkLogic` processor that will call the Smart Mastering REST extension for processing match and merge on a set of documents.

## Processors

### ListFile

**Properties**

Schema Output Destination
 : /<path-to>/smart-mastering-core/examples/dhf-flow/data/Organizations/Source1/

File Filter
 : .\*\\.json

**Settings**

*Default*

### FetchFile

**Properties**

*Default*

**Settings**

Automatically Terminate Relationships
 : failure, not.found, permission.denied

**Relationships**

Link `success` from "ListFile" to "FetchFile"

### PutMarkLogic

**Properties**

DatabaseClient Service
 : *Staging Database Client*

Server Transform
 : mlSjsInputFlow

URI Prefix
 : /organizations/

URI Suffix
 : .json

trans:entity-name
 : Organization

trans:flow-name
 : OrgImportSource1

**Settings**

Automatically Terminate Relationships
 : success, failure

**Relationships**

Link `success` from "FetchFile" to "PutMarkLogic"

### ExtensionCallMarkLogic *DHF Harmonize*

**Properties**

DatabaseClient Service
 : *Staging Database Client*

Extension Name
 : mlSjsFlow

Payload Source
 : Payload Property

Payload Format
 : JSON

Payload
 : {}

param:entity-name
 : MDM

param:flow-name
 : MDMHarmonizeSJS

param:target-database
 : data-hub-FINAL

param:identifiers
 : ${URIs}

separator:param:identifiers
 : ,

**Settings**

Name
 : DHF Harmonize

Automatically Terminate Relationships
 : failure

**Relationships**

Link `batch_success` from "PutMarkLogic" to "DHF Harmonize"

### ExtensionCallMarkLogic *Smart Mastering*

**Properties**

DatabaseClient Service
 : *Final Database Client*

Extension Name
 : sm-match-and-merge

Payload Source
 : None

Payload Format
 : TEXT

param:options
 : org-merge-options

param:query
 : \<cts:collection-query xmlns:cts="http://marklogic.com/cts"\>\<cts:uri\>Organization\</cts:uri\>\</cts:collection-query\>

param:uri
 : ${URIs}

separator:param:uri
 : ,

**Settings**

Name
 : Smart Mastering

Automatically Terminate Relationships
 : success, failure

**Relationships**

Link `success` from "DHF Harmonize" to "Smart Mastering"

[smart-mastering-dhf-example]: https://github.com/marklogic-community/smart-mastering-core/tree/v1.2.2/examples/dhf-flow
[dhf-sm-template]:./files/DataHubAndSmartMasteringMarkLogicFlow.xml
