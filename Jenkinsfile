@Library('shared-libraries') _
pipeline{
  agent {label 'devExpLinuxPool'}
  options {
    checkoutToSubdirectory 'nifi-connector'
    buildDiscarder logRotator(artifactDaysToKeepStr: '7', artifactNumToKeepStr: '', daysToKeepStr: '30', numToKeepStr: '')
  }
  environment{
    JAVA_HOME_DIR="/home/builder/java/jdk-11.0.2"
    GRADLE_DIR   =".gradle"
    MAVEN_HOME_DIR="/home/builder/mvn/apache-maven-3.8.6/"
    DMC_USER     = credentials('MLBUILD_USER')
    DMC_PASSWORD = credentials('MLBUILD_PASSWORD')
  }
  stages{
    stage('tests'){
      steps{
        copyRPM 'Release','10.0-9.4'
        setUpML '$WORKSPACE/xdmp/src/Mark*.rpm'
        sh label:'deploy project', script: '''#!/bin/bash
          export JAVA_HOME=$JAVA_HOME_DIR
          export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
          export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
          cd nifi-connector/test-app
          echo mlPassword=admin >> gradle-local.properties
          ./gradlew mlDeploy
        '''
        sh label:'test', script: '''#!/bin/bash
          export JAVA_HOME=$JAVA_HOME_DIR
          export MAVEN_HOME=$MAVEN_HOME_DIR
          export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH
          cd nifi-connector
          mvn clean verify  || true
        '''
        junit '**/*.xml'
      }
    }
  }
}
