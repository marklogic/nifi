# MarkLogic NiFi connector

The MarkLogic NiFi connector simplifies integrating [Apache NiFi](https://nifi.apache.org/) with MarkLogic, allowing for 
data to be easily written to and read from MarkLogic. The connector consists of a set of custom NiFi processors and 
controller services which can be used in NiFi flows for integrating with MarkLogic. The connector has been developed 
and tested on NiFi 1.15.3; it may work in more recent versions of NiFi too. 

Please see the [Getting Started guide](https://marklogic.github.io/nifi/getting-started) 
for information on obtaining the connector, installing it, and using it. 


## Building and testing the connector

If you'd like to build the MarkLogic NiFi connector from source, you'll first need to 
[download and install Apache Maven](https://maven.apache.org/) if you do not already have it installed. 

As of the 1.15.3.2 release, Java 11 should be used to run the Maven commands below. The Maven pom.xml file now ensures
that the NAR files constructed by Maven will run on Java 8, but it does so via a compiler switch introduced in Java 9. 
Thus, Java 8 cannot be used to run Maven, but the NAR files will run on both Java 8 and Java 11. This is consistent
with the [NiFi system requirements](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#system_requirements).

Next, clone this repository and run the following command to build the two NAR files:

    mvn clean install -DskipTests

It is recommended to use "-DskipTests" unless you have completed the instructions below for deploying an application to 
MarkLogic that the tests depend on. You may still want to include "-DskipTests" so that the process for building the NARs
is faster. 

After "install" completes, the below NARs will have been created:

- ./nifi-marklogic-nar/target/nifi-marklogic-nar-(version).nar
- ./nifi-marklogic-services-api-nar/target/nifi-marklogic-services-api-nar-(version).nar

You can then copy these NAR files into your NiFi installation as described in the 
[Getting Started guide](https://marklogic.github.io/nifi/getting-started).

Note that depending on the version of NiFi you're running, NiFi may require you to login after accessing the NiFi 
home page after starting it up. See [these NiFi docs](https://nifi.apache.org/docs/nifi-docs/html/getting-started.html#i-started-nifi-now-what)
for instructions on how to login.


Running the tests
=========

Please note that the instructions right below this only run the JUnit 4 tests within this project. The project
also contains JUnit 5 tests, but the necessary Maven config has not been determined yet to allow for both sets of tests
to be run when running the Maven "test" task. If you would like to run all of the tests - which is important when 
developing the connector - please use an IDE such as IntelliJ that is able to run both the JUnit 4 and JUnit 5 tests 
at the same time.

After cloning this repository locally and installing Maven, you can run the tests for the connector by performing the 
following steps (as noted above, be sure to use Java 8):

1. cd nifi-marklogic-processors
1. Put your ML admin username/password in gradle-local.properties (a gitignored file, so you'll need to create it)
1. Run ./gradlew -i mldeploy (uses Gradle to deploy a small test application to ML)
1. cd ..
1. Run "mvn clean test"

You should have output like this:

```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for nifi-marklogic-bundle 1.15.3:
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
```
