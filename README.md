Embedded Sql Database
===============================
*EmbeddedSqlDatabase* provides services over JDBC. It can be used with any JDBC
factory driver (news drivers must be added to pom.xml). If no specif driver was
defined, *EmbeddedSqlDatabase* automaticaly starts one of memory embedded   SQL 
database, like H2 SQL 

###1. Technologies used
* Maven 4.0
* H2 1.4.192
* Log4J 1.2.17
* Gson 2.7

###2. To clone this project locally
```shell
$ git clone https://github.com/josemarsilva/embeddedSqlDatabase
```

###3. To import this project into Eclipse IDE
1. ```$ mvn eclipse:eclipse```
2. Import into Eclipse via **existing projects into workspace** option.
3. Done.

###4. Project Demo
Run the following method from this class:
* class: org.josemarsilva.poc.embeddedSqlDatabase.Main
* method: main()
* expected resource: a little demonstration of use of *embeddedSqlDatabase*

###5. Documentation
* ./doc: Contains some UML diagrams built with Astah Communit (.asta)
