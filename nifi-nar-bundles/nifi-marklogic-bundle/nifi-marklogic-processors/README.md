In order to run all of the tests in this project, you must first deploy a test application to MarkLogic:

Execute the commands as below :

1. ./gradlew -i mlDeploy
2. cd ../dhf5 && ./gradlew -i mlDeploy

Tests that do not end in "IT" do not depend on the test application having been deployed. 
