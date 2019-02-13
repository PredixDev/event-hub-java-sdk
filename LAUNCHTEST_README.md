# Table of Contents 

 - Introduction 
 - Launching Test   
 - Launching Smoke Tests for Production and external github branches 

## Introduction
All the existing tests have been categorized into Sanity Tests via annotation @Category(SanityTest.class). 
Further there are more categories for new tests or existing test that could be categorized further into correct bucket. Available categories are as follows 
 - SmokeTest - @Category(SmokeTest.class) 
 - IntegrationTest - @Category(IntegrationTest.class)
 - RegressionTest - @Category(RegressionTest.class)
 - DefectTest - @Category(DefectTest.class)

Once could either categorize a single test via these annoations by adding the annoation on the test or can categorize all the tests within a class by annotating at class level. e.g. AllSmokeTests.java . A single test can also be part of different category's by defining the categoery it belongs to e.g. @Category({SmokeTest.class,SanityTest.class}) 


## Launching Tests
To run tests you need to associate a profile. e.g. "mvn test -P $PROFILENAME". This profilename is lets you pick which category of tests you want to launch. The profiles are below
- -P Smoke tied to SmokeTest
- -P Sanity tied to SanityTest
- -P Integration tied to IntegrationTest
- -P Regression tied to RegressionTest
- -P Defect tied to DefectTest

However there is a default profile that is tied to that, which is SmokeTest. This will be picked up when you launch "mvn clean install". 
If you want to run a different category of Tests you can launch via mvn $TARGET -P Sanity ($TARGET could be clean,compile,test,install)

You can also launch multiple profile tests together. e.g. mvn test -P Smoke,Sanity

To Run all Tests  "mvn test -P Sanity" 

## Launching Smoke Tests for Production and external github branches
 To simply launching tests on these environments and be able to leverage same tests across different environment targets without making use of exporting environment variables we have provision this via use of properties file. The properties file takes only few variables that should be enough to launch the tests against different environments. 
There are two properites file aiding to help you launch the tests against different environments from a single point without having to edit the same property file while flipping between target enviornments. 
 
 1.)  event-hub.properties tied to env variable environemnt=production .    
 
 2.) event-hub-test.properties file tied to env variable environment=test .
 
 If the env variable is not set; it will default to environement=test and read the corresponding file to gather the key value pairs. 

Contents of the properties file, but not limited to the following. You are free to add more property values and add a test that consumes that. 
 - EVENTHUB_URI=       - Mandatory, however Either this needs to be present or EVENT_HOST needs to be present
 - EVENTHUB_HOST=      - Same as above
 - AUTH_URL=	    - Mandatory
 - CLIENT_ID=	    - Mandatory
 - CLIENT_SECRET=	    - Mandatory
 - ZONE_ID=	    - Mandatory
 - EVENTHUB_PORT=	    - Mandatory
 - NUMBER_OF_MESSAGES= - If not specified, defaults to 1. 

### e.g of Launching Smoke Tests to make use of event-hub.properties

1.) Step 1 : export environment=production

2.) Step 2 : mvn test -P Smoke

You should see the following log messages

2018-12-13 17:48:29 INFO  AllSmokeTest:71 -  Environment used for testing : PRODUCTION

2018-12-13 17:48:29 INFO  PropertiesConfiguration:84 - Looking for file event-hub.properties at path 


### e.g. Launching Smoke Tess to make use of event-hub-test.properties

1.) Step 1 (Optional): export environment=test

2.) Step 2 : mvn test -P Smoke

You should see the following log messages

2018-12-14 10:19:58 INFO  AllSmokeTest:71 -  Environment used for testing : TEST

2018-12-14 10:19:58 INFO  PropertiesConfiguration:84 - Looking for file event-hub-test.properties at path .

