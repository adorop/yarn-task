#YARN Task

##Description
Finds 3 the most popular hotels between couples using Yarn API and Spring Boot Yarn support.

##Structure
Can be divided on 2 major parts:
#####Engine
Provides runtime for _application master_ and _container_, not tied to a particular application
* _yarn-task-engine-appmaster_: divides work defined in subclass of 
[YarnApplicationConfigurerAdapter](yarn-task-engine-appmaster/src/main/java/com/aliaksei/darapiyevich/yarntask/engine/appmaster/configuration/YarnApplicationConfigurerAdapter.java) into stages which are scheduled by 
[ApplicationMaster](yarn-task-engine-appmaster/src/main/java/com/aliaksei/darapiyevich/yarntask/engine/appmaster/ApplicationMaster.java)
* _yarn-task-engine-container_: reads [TaskDefinition](yarn-task-engine-contract/src/main/java/com/aliaksei/darapiyevich/yarntask/engine/contract/definition/TaskDefinition.java)
provided by _application master_ and translates it in the internal runtime
* _yarn-task-engine-contract_: contains classes used by _application master_ and _container_ for communication

#####Application itself
Enables _engine_, provides application specific logic and configuration

* _yarn-task-client_: enables Spring Boot YARN support and provides information required for proper start of _application master_ 
in [application.yml](yarn-task-client/src/main/resources/application.yml)
* _yarn-task-appmaster_: describes application specific logic in 
[YarnApplicationMasterConfiguration](yarn-task-appmaster/src/main/java/com/aliaksei/darapiyevich/yarntask/appmaster/configuration/YarnApplicationMasterConfiguration.java)
* _yarn-task-container_: enables _container engine runtime_ by placing [@EnableYarnContainer](yarn-task-engine-container/src/main/java/com/aliaksei/darapiyevich/yarntask/engine/container/configuration/EnableYarnContainer.java) on Spring's _Configuration_ class

##Build

```
$ mvn package
```
will build **yarn-task-client**, **yarn-task-appmaster** and **yarn-task-container** _fat_ jars required to run the application

##Run

```
$ java -jar yarn-task-client-1.0.jar
``` 

will run _yarn client_ application which will launch _application master_ application.   
However, each application requires worth of properties which can be provided via _application.yml/properties_, system properties or environment variables according to Spring _@ConfigurationProperties_ semantics: 
* _yarn-task-client_:  
  * _spring.hadoop.resourceManagerHost_ property
* _yarn-task-appmaster_:
  * read.path
  * write.path
  * aggregate.parallelism
  * spring.hadoop.fsUri
* _yarn-task-container_:
  * spring.hadoop.fsUri
  
To run on HDP simple [sh script](run_on_hdp.sh) has been created
