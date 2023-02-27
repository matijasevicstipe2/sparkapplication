# Spark Challenge

This project demonstrates the implementation of Spark application (in Scala) that processes 2 CSV files:
* googleplaystore.csv
* googleplaystore_user_reviews.csv


### Technologies

* Java 11.0.9
* Scala 2.12
* Spark 3.3.1
* Maven

### Installation and startup instructions

To get started with this project, you will need to have the following on your local machine:

* JDK 11+
* In System variables you should have JAVA_HOME variable pointed to the JDK installed folder
* In System variables you should have SPARK_HOME variable pointed to the Spark installed folder
* In System variables you should have HADOOP_HOME variable pointed to the Hadoop installed folder

To build and run the project, navigate to the directory where the jar file is located and execute the following command:

    spark-submit jarfilename.jar

If the JAR file to run is located in a different folder, you’ll need to provide a full path to the file.
