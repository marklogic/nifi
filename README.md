There'll be more content here soon, but for now, here's how to make sure you're able to build the processors locally
and verify the tests pass:

1. cd nifi-marklogic-processors
1. Put your ML admin username/password in gradle-local.properties (a gitignored file, so you'll need to create it)
1. Run ./gradlew -i mldeploy (deploys a small test application to ML)
1. cd .. 
1. Run "mvn clean install"

You should have output like this:

```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for nifi-marklogic-bundle 1.9.1.5-SNAPSHOT:
[INFO] 
[INFO] nifi-marklogic-bundle .............................. SUCCESS [  1.062 s]
[INFO] nifi-marklogic-services-api ........................ SUCCESS [  1.032 s]
[INFO] nifi-marklogic-services-api-nar .................... SUCCESS [  0.366 s]
[INFO] nifi-marklogic-services ............................ SUCCESS [  2.692 s]
[INFO] nifi-marklogic-processors .......................... SUCCESS [  5.757 s]
[INFO] nifi-marklogic-nar ................................. SUCCESS [  1.235 s]
[INFO] nifi-marklogic-services-nar ........................ SUCCESS [  0.506 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  13.823 s
[INFO] Finished at: 2021-01-20T18:25:11-05:00
[INFO] ------------------------------------------------------------------------

```
