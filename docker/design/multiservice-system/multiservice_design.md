<!-- vscode-markdown-toc -->
* 1. [Vision](#Vision)
* 2. [Requirements break-down](#Requirementsbreak-down)
	* 2.1. [Functional requirements](#Functionalrequirements)
	* 2.2. [Non-functional requirements](#Non-functionalrequirements)
	* 2.3. [Non requirement](#Nonrequirement)
* 3. [Architecture](#Architecture)
	* 3.1. [Architecture diagram](#Architecturediagram)
	* 3.2. [Architecture principles](#Architectureprinciples)
* 4. [Use cases walkthrough](#Usecaseswalkthrough)
* 5. [Important scenrios](#Importantscenrios)
* 6. [Component design](#Componentdesign)
* 7. [Database design](#Databasedesign)
* 8. [Test cases](#Testcases)
* 9. [Implementation](#Implementation)
* 10. [System Troubleshoot and Visibility](#SystemTroubleshootandVisibility)
* 11. [Bugs](#Bugs)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->
# Multi service plugin concurrent subsystem

##  1. <a name='Vision'></a>Vision
Multiple clients like to contribute in multiple services (example listed below)
1. DM Check service
2. Follower check service
3. Following check service
4. A new service
These clients can come and go anytime. System should provide fault tolerant and efficient solution for them to contribute.


##  2. <a name='Requirementsbreak-down'></a>Requirements break-down
###  2.1. <a name='Functionalrequirements'></a>Functional requirements
* Multiple clients should be able to register for one or more services
* A service may be added anytime 
* System should facilitate for revoking of a service. 
* In case any service is revoked, past activity related to service must be preserved.
* A registered client can only work on a valid service
* System should be able to deactivate clients for services
[Pending]
* Client should able to check if a service is valid or not
* Multiple clients should be able to proxy using same Tweeter ID. However, in this case, its  the client responsibility to maintain harmony among themselves. System will treat all such clients as identical [Pending]
* Faulty client can submit incorrect info. System should allow to recover in such case. As an approach, system can log client activity [Pending]


###  2.2. <a name='Non-functionalrequirements'></a>Non-functional requirements
* System should able to add new services using plugin model.
* System should  deactivate inactive clients for services.
* Client needs to activate service in case it comes back again
* Client should not wait for registration of service
* System should be able to scale with reasonable limit. Limit should be advertised [Pending]
* System should be lock-free. Lock must be used only when it can't be avoided. Its requirement for scale. Lock will be bottleneck for any system with lock 
* System should facilitate enough data for monitoring and troubleshooting [Pending]
* System should be able to minimize impact of erroneous client. A repeated registratration request from same client should not affect other clients.
* System should keep enough stats for client access to service

###  2.3. <a name='Nonrequirement'></a>Non requirement
* System doesn't support grouping of services

##  3. <a name='Architecture'></a>Architecture
Concurrent processing of each service can be mapped to public distribution system(PDS). Note that PDS shops gives the fix amount of groceriers to multiple card holders. Card holder can be anyone who has government approval. Generally on distribution day, there will be queue. To speedize, this shop owner makes bucket of rations as pre-processing. This helps shop owner to distribute ration in parallel.


###  3.1. <a name='Architecturediagram'></a>Architecture diagram

![image info](./data/jpg/MultiServiceClients!Architecture!Multi service registration_20.jpg)
![image info](./data/jpg/MultiServiceClients!Architecture!MultiServiceComponents_19.jpg)

###  3.2. <a name='Architectureprinciples'></a>Architecture principles
1. Lock free concurrency (for scale)
2. Linear span- optimal number of variables (for stability, maintanenence)
3. No duplication of data
4. Simple in design and simple in implementation

##  4. <a name='Usecaseswalkthrough'></a>Use cases walkthrough
![image info](./data/jpg/MultiServiceClients!Use-cases!Use-cases_16.jpg)

##  5. <a name='Importantscenrios'></a>Important scenerios

##  6. <a name='Componentdesign'></a>Component design


##  7. <a name='Databasedesign'></a>Database design

![image info](./data/jpg/MultiServiceClients!database system!Database design complete!database design_complete_17.jpg)

![image info](./data/jpg/MultiServiceClients!database system!database design custom!Database design custom!Database design cu_21.jpg)

![image info](./data/jpg/MultiServiceClients!database system!databse design basic!Data Model1!database_design_basic_18.jpg)

##  8. <a name='Testcases'></a>Test cases


##  9. <a name='Implementation'></a>Implementation


##  10. <a name='SystemTroubleshootandVisibility'></a>System Troubleshoot and Visibility 

##  11. <a name='Bugs'></a>Bugs
