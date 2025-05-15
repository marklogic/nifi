This guide describes how to develop and contribute pull requests to this connector. The guide first addresses how to 
develop and test the connector, and then addresses how to submit a pull request.

# Developing and testing the connector

## Building and installing the connector

You'll first need to [download and install Apache Maven](https://maven.apache.org/) if you do not already have it
installed.

As of the NiFi 2.0.0 release, Java 21 must be used to run the Maven commands below. This is consistent
with the [NiFi system requirements](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#system_requirements).

Next, clone this repository (if you haven't already) and run the following command to build the two NAR files:

    mvn clean install -DskipTests

It is recommended to use "-DskipTests" when building the NARs for the purpose of testing them in NiFi. The tests should
certainly be run throughout the development process and before submitting a pull request. But for manually testing the
connector via NiFi, it's usually acceptable to skip running the tests to avoid the delay associated with running them.

After `install` completes, the below NARs will have been created:

- ./nifi-marklogic-nar/target/nifi-marklogic-nar-(version).nar
- ./nifi-marklogic-services-api-nar/target/nifi-marklogic-services-api-nar-(version).nar

## Getting setup via Docker

If you have Docker Desktop installed, just run the following (make sure you've already built the nar files though as 
described above):

    docker-compose up -d --build

This will create a "marklogic_nifi" service with "marklogic" and "nifi" containers. You can then go to 
https://localhost:8443/nifi and login as admin/password1234. The nar files that you built above will be mapped to NiFi's
"lib" directory so you don't need to do anything further to start using the connector in NiFi. The nar version is defined 
in the .env file at the root of the project. Make sure that value matches your build version in the POM.

The NiFi log files are mapped to `./docker/nifi/logs`. You can run the following to tail the NiFi log files:

    tail -f docker/nifi/logs/nifi-bootstrap.log docker/nifi/logs/nifi-app.log


## Getting setup with a local NiFi

If you don't want to use docker-compose, follow these instructions.

See [the NiFi installation docs](https://nifi.apache.org/docs.html) for instructions on installing NiFi. For Mac users,
the homebrew installation approach is recommended.

After installing NiFi, set the `NIFI_HOME` environment variable to the directory where you've installed NiFi. This
environment variable will be referred to frequently in this guide. 

You can then copy these NAR files into your NiFi installation as described in the
[Getting Started guide](https://marklogic.github.io/nifi/getting-started).

Before starting NiFi for the first time, you should set an admin password that will be used when logging into the 
NiFi web interface. See [these NiFi docs](https://nifi.apache.org/docs/nifi-docs/html/getting-started.html#i-started-nifi-now-what) 
for instructions on how to configure this password.

You can then start NiFi using Java 21 via the following command:

    $NIFI_HOME/bin/nifi start

During development, it is very helpful to tail the NiFi log files, particularly when using the `LogAttribute` NiFi
processor to examine the contents of `FlowFiles`. You can do so via the following:

    tail -f $NIFI_HOME/libexec/logs/nifi-app.log $NIFI_HOME/libexec/logs/nifi-user.log

After starting NiFi, you can access its web interface at https://localhost:8443/nifi . You can login with the username
and password that you configured above.

## Using NiFi

If you have not used NiFi before or are fairly new to it, you may find it helpful to walk through
[the NiFi Getting Started guide](https://nifi.apache.org/docs.html) and some of the 
[connector's recipes](https://marklogic.github.io/nifi/cookbook-recipes) to learn the basics of NiFi and the
connector. 

## Test flows

The file `./nifi-marklogic-processors/flows-for-manual-testing.json` file contains several flows that can be used
for manual testing. To try these flows out, perform the following steps:

1. In NiFi, click on the "Process Group" icon in the header (it has a box with two smaller boxes in it) and drag it
onto the canvas.
2. Click on the "Browse" icon in the "Process Group Name" selector.
3. Select the `flows-for-manual-testing.json` file.
4. Open the "flows-for-manual-testing" process group that is now on the NiFi canvas.
5. Right-click on the "flows-for-manual-testing" process group and select "Controller Services."
6. Click on the ellipsis on the right-hand side of the "test-marklogic-nifi-8006" controller service and select "Edit."
7. Enter "admin" for the "Password" field.
8. Click on the "Enable" icon for the controller service.

You can now try each of the flows in the process group. Each flow has a note in it to help with testing.

## Updating the connector

After any modification to the connector source code, the connector NAR files must be rebuilt, copied to the NiFi 
installation directory, and then NiFi must be restarted. 

If you are using Docker, you can run the following to rebuild the connector and restart the NiFi container:

    mvn clean install -DskipTests
    docker container restart nifi

For a local NiFi installation, it is recommended to automate the process via a shell function (or script) like the one 
below, which requires the connector version number as an input (you can name this whatever you'd like, or implement it 
in a different fashion, it's simply excluded for sake of example):

```
function nifirebuild {
  $NIFI_HOME/bin/nifi stop
  mvn clean install -DskipTests
  rm -f $NIFI_HOME/libexec/lib/nifi-marklogic*
  rm -rf $NIFI_HOME/libexec/work/docs/components/org.apache.nifi/nifi-marklogic-nar
  cp nifi-marklogic-nar/target/nifi-marklogic-nar-*.nar $NIFI_HOME/libexec/lib
  cp nifi-marklogic-services-api-nar/target/nifi-marklogic-services-api-nar-*.nar $NIFI_HOME/libexec/lib
  $NIFI_HOME/bin/nifi start
  tail -f $NIFI_HOME/libexec/logs/nifi-app.log $NIFI_HOME/libexec/logs/nifi-user.log
}
```

Based on how you've installed NiFi, you may need to adjust the NAR library path - i.e. `$NIFI_HOME/libexec/lib` will 
work if you've installed NiFi homebrew, but otherwise, you likely will need `$NIFI_HOME/lib`.

A note on the above script - the reason for deleting the `libexec/work/docs/components/org.apache.nifi/nifi-marklogic-nar`
directory is to ensure that changes to documentation in the connector components can be verified after a rebuild. 
Otherwise, NiFi appears to cache connector documentation for a particular version in this directory, and that cache 
is not updated when the connector is modified. 

## Running the tests

This project contains both unit and integration tests. The integration tests depend on a small MarkLogic application
that must first be deployed to MarkLogic via [ml-gradle](https://github.com/marklogic-community/ml-gradle). The 
application is deployed via the following steps:

1. `cd test-app`
2. If running a native MarkLogic install, verify that the admin password in the `gradle.properties` file is correct,
overriding it in `gradle-local.properties` as needed.
3. Run `./gradlew hubInit` to initialize the DHF project; a DHF project is needed in order to test the `RunFlowMarkLogic` processor.
4. Run `./gradlew -i mldeploy`

You can then run all tests in the project by returning to the parent directory and running `verify`:

    cd .. 
    mvn verify

Or run `mvn clean verify` to first perform a clean build.

You should see logging from all the Maven phases, including logging from tests, which then concludes with a summary
like the one below:

```
[INFO] Reactor Summary for nifi-marklogic-bundle 2.0.0:
[INFO] 
[INFO] nifi-marklogic-bundle .............................. SUCCESS [  1.179 s]
[INFO] nifi-marklogic-services-api ........................ SUCCESS [  1.208 s]
[INFO] nifi-marklogic-services-api-nar .................... SUCCESS [  1.175 s]
[INFO] nifi-marklogic-services ............................ SUCCESS [  1.752 s]
[INFO] nifi-marklogic-processors .......................... SUCCESS [01:10 min]
[INFO] nifi-marklogic-nar ................................. SUCCESS [  2.861 s]
[INFO] nifi-marklogic-services-nar ........................ SUCCESS [  1.752 s]
```

You can also only run the unit tests, which do not make any calls to the MarkLogic application installed above:

    mvn test

## Testing the documentation locally

The docs for this project are stored in the `./docs` directory as a set of Markdown files. These are published via
[GitHub Pages](https://docs.github.com/en/pages/getting-started-with-github-pages/about-github-pages) using the
configuration found under "Settings / Pages" in this repository.

You can build and test the docs locally by
[following these GitHub instructions](https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/testing-your-github-pages-site-locally-with-jekyll),
though you don't need to perform all of those steps since some files generated by doing so are already in the
`./docs` directory. You just need to do the following:

1. Install the latest Ruby (rbenv works well for this).
2. Install Jekyll.
3. Go to the docs directory - `cd ./docs` .
4. Run `bundle install` (this may not be necessary due to Gemfile.lock being in version control).
5. Run `bundle exec jekyll serve`.

You can then go to <http://localhost:4000> to view the docs.

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

