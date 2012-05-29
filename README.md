Vertica CLI Tool
=================

A simple CLI tool written in Java for connecting to a Vertica DB using their JDBC driver.

The need for this tool was to test multiple property options to achieve the best possible
performance (based on particular usage). One property that has been changed is the Max LRS
Memory option. THis sets the buffer to a very low value meaning that the Vertica server
has to start streaming results immediately, which is very useful when dealing with extremely
large result sets.

Maven Setup
------------

This project has a pom which will build the tool.

You will need to download and install [maven](http://maven.apache.org/download.html).

Once installed, you can build the project by going to the root of the project and entering the following command:

	mvn package

Once complete the required Jar will be available within the target folder.

To run the tool you can enter the following command:

	java -jar target/vertica-cli.jar <jdbc-url> <username> <password>