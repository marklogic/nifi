In order to run all of the tests in this project, you must first deploy a test application to MarkLogic:

    ./gradlew -i mlDeploy

This ensures that all of the integration tests - those ending in "IT" - will be able to run. 

Tests that do not end in "IT" do not depend on the test application having been deployed. 