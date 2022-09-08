This guide describes how to develop and contribute pull requests to this connector. The guide first addresses how to 
develop and test the connector, and then addresses how to submit a pull request.

# Developing and testing the connector

## Installing NiFi

See [the NiFi installation docs](https://nifi.apache.org/docs.html) for instructions on installing NiFi. For Mac users,
the homebrew installation approach is recommended.

After installing NiFi, set the `NIFI_HOME` environment variable to the directory where you've installed NiFi. This
environment variable will be referred to frequently in this guide. 

## Building and installing the connector

You'll first need to [download and install Apache Maven](https://maven.apache.org/) if you do not already have it 
installed.

As of the 1.15.3.2 release, Java 11 should be used to run the Maven commands below. The Maven pom.xml file now ensures
that the NAR files constructed by Maven will run on Java 8, but it does so via a compiler switch introduced in Java 9.
Thus, Java 8 cannot be used to run Maven, but the NAR files will run on both Java 8 and Java 11. This is consistent
with the [NiFi system requirements](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#system_requirements).

Next, clone this repository (if you haven't already) and run the following command to build the two NAR files:

    mvn clean install -DskipTests

It is recommended to use "-DskipTests" when building the NARs for the purpose of testing them in NiFi. The tests are
typically run separately while developing the connector and often via an IDE. 

After "install" completes, the below NARs will have been created:

- ./nifi-marklogic-nar/target/nifi-marklogic-nar-(version).nar
- ./nifi-marklogic-services-api-nar/target/nifi-marklogic-services-api-nar-(version).nar

You can then copy these NAR files into your NiFi installation as described in the
[Getting Started guide](https://marklogic.github.io/nifi/getting-started).

## Starting NiFi and logging in

Before starting NiFi for the first time, you should set an admin password that will be used when logging into the 
NiFi web interface. See [these NiFi docs](https://nifi.apache.org/docs/nifi-docs/html/getting-started.html#i-started-nifi-now-what) 
for instructions on how to configure this password.

You can then start NiFi using Java 11 via the following command:

    $NIFI_HOME/bin/nifi start

During development, it is very helpful to tail the NiFi log files, particularly when using the `LogAttribute` NiFi
processor to examine the contents of `FlowFiles`. You can do so via the following:

    tail -f $NIFI_HOME/libexec/logs/nifi-app.log $NIFI_HOME/libexec/logs/nifi-user.log

After starting NiFi, you can access its web interface at https://localhost:8443/nifi . You can login with the username
and password that you configured above.

If you have not used NiFi before or are fairly new to it, you may find it helpful to walk through
[the NiFi Getting Started guide](https://nifi.apache.org/docs.html) and some of the 
[connector's recipes](https://marklogic.github.io/nifi/cookbook-recipes) to learn the basics of NiFi and the
connector. 

## Updating the connector

After any modification to the connector source code, the connector NAR files must be rebuilt, copied to the NiFi 
installation directory, and then NiFi must be restarted. Because this process happens so often, it is recommended to 
automate it via a shell function (or script) like the one below, which requires the connector version number as an 
input (you can name this whatever you'd like, or implement it in a different fashion, it's simply excluded for sake of
example):

```
function nifirebuild {
  $NIFI_HOME/bin/nifi stop
  mvn clean install -DskipTests
  rm -f $NIFI_HOME/libexec/lib/nifi-marklogic*
  cp nifi-marklogic-nar/target/nifi-marklogic-nar-*.nar $NIFI_HOME/libexec/lib
  cp nifi-marklogic-services-api-nar/target/nifi-marklogic-services-api-nar-*.nar $NIFI_HOME/libexec/lib
  $NIFI_HOME/bin/nifi start
  tail -f $NIFI_HOME/libexec/logs/nifi-app.log $NIFI_HOME/libexec/logs/nifi-user.log
}
```

Based on how you've installed NiFi, you may need to adjust the NAR library path - i.e. `$NIFI_HOME/libexec/lib` will 
work if you've installed NiFi homebrew, but otherwise, you likely will need `$NIFI_HOME/lib`.

## Running the tests

Please note that the instructions right below this only run the JUnit 4 tests within this project. The project
also contains JUnit 5 tests, but the necessary Maven config has not been determined yet to allow for both sets of tests
to be run when running the Maven "test" task. If you would like to run the tests - which is important when
developing the connector - please use an IDE such as IntelliJ that is able to run both the JUnit 4 and JUnit 5 tests
at the same time.

After cloning this repository locally and installing Maven, you can run the tests for the connector by performing the
following steps (as noted above, be sure to use Java 8):

1. `cd nifi-marklogic-processors`
1. Put your ML admin username/password in gradle-local.properties (a gitignored file, so you'll need to create it)
1. Run `./gradlew -i mldeploy` (uses Gradle to deploy a small test application to ML)
1. `cd ..`
1. Run `mvn clean test`

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

# Submitting a pull request

These instructions are intentionally brief and assume a working knowledge of GitHub and pull requests. 

To submit a pull request (PR) to this repository, please do the following:

1. Fork this repository
2. Ensure there is a GitHub issue already for the problem you intend to solve; create one if it doesn't yet exist
3. Create a branch off the `develop` branch; you can name the branch whatever you'd like, though it's recommended to
  follow the Gitflow convention of `feature/(issue numnber)-(very brief description)`.
4. When you are ready to submit a PR, squash all your commits on your feature branch into one commit with a brief title
  and a thorough description of the changes in the commit. Ensure that you have sufficient test coverage of all 
  functionality and/or fixes introduced by the commit.
5. Submit the PR to this repository with the `develop` branch as the target.

