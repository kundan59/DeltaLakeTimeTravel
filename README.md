# Time travel with delta-lake
Databricks Delta, the next-gen unified analytics engine built on top of Apache Spark, introduces unique Time Travel capabilities. 

The project consists of 3 java files:
1. CreateJobs - This is a sample class which runs two jobs. Job-1 writes to delta lake and Job-2 updates the record written by Job-1.
It consists of methods which are mostly used while a job is related to apache spark and write into delta lake.  
2. DeltaLakeTimeTravel - This is the main class which would execute the sample class using the methods provided in TimeTravelApproaches. 
3. TimeTravelApproaches - This class contains the methods required to read a delta table using:
a. version number - timeTravelUsingVersionNumber()
b. timestamp - timeTravelUsingTimeStamp()
It also contains a method to get the audit log of the changes made to the table - getLatestHistory()

## Table of contents  
1. [Getting Started](#Getting-Started) 
2. [How to Run](#How-to-Run) 
3. [How to Use](#How-to-Use)  
 

  
## Getting Started  
#### Minimum requirements  
To run the SDK you will need  **Java 1.8+, Scala 2.11.8 **.   Also you need to install spark** as prerequisites
  
#### Installation  
The way to use this project is to clone it from github and build it using maven.

## How to Run 
Open terminal in project folder and execute these commands:
1. ```mvn clean install```
2. ```mvn exec:java```
After running the project, you should see execution outputs of running sample job 1 and job 2, which updates the delta table written by job 1.
Finally, you can see the original delta table created by job 1 using version number and then using timestamp.

## How to Use
You need to instantiate TimeTravelApproaches() class in your project and call the required method using parameters.
