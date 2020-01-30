# Time travel with delta-lake
  
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
2. [How to Use](#How-to-Use)  
3. [How to Run](#How-to-Run)  

  
## Getting Started  
#### Minimum requirements  
To run the SDK you will need  **Java 1.8+, Scala 2.11.8 **.   Also you need to install spark** as prerequisites
  
#### Installation  
The way to use this project is to clone it from github and build it using sbt.  
  
## How to use   
We need to extend the `StreamingDriver` trait and override the run method and write the job operations there.  

  
```scala  
object ExampleDeltalake extends StreamingDriver {

 import InputSource.sparkSession.implicits._

 override def run(dataSet: Dataset[String]) = {

    // your codes here

   }

}

```  
  
#### Abstract Methods  
```scala 
 
override def run(dataSet: Dataset[String]) : Unit

```  
  
#### Concrete Methods  
  
The available concrete methods are -   
  
```scala  
def takeInput(): DataFrame
 
def writeToDelta(outputDataFrame: DataFrame, filePath: String, checkPointPath: String)

```  
## How to Run 


