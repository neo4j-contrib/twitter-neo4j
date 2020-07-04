
# DM check subsystem

## Vision
Multiple clients like to contribute in checking openDM. These clients can come and go anytime. System should provide fault tolerant and fast soltion for them to contribute.

# Requirements break-down
## Functional requirements
* Multiple clients should be able to fetch user list for checking
* Only the client who got the user list should be able to update
* Only registered client should be able to interact with system
* In case of any difficulyt(Crash for example), Client should be able to recollect its owned user list with the help of system
* User should be able to give list of users which needs high priority for processing OpenDM
* User should be able to view list of users which are not yet marked for processing.

## Non-functional requirements
* Clients should not wait for getting user list
* Client should not wait while updaing openDM info for user
* System should be fair to users for OpenDM check. It means that no user openDM check should be starved for ever
* System should unlock user lists owned by dead clients and reassigns
* System should have minimal load on DB while its processing
* System should have a resonable maximum wait time for processing any user openDM once it marked for processing. For non-marked, user, its fine as user can assing if needed.
* System should be able to scale with reasonable limit. Limit should be advertised


## Architecture
This problem can be mapped to public distribution system(PDS). Note that PDS shops gives the fix amount of groceriers to multiple card holders. Card holder can be anyone who has government approval. Generally on distribution day, there will be queue. To speedize, this shop owner makes bucket of rations as pre-processing. This helps shop owner to distribute ration in parallel.

## Use cases walkthrough
### Below use-case talks about how the users are divided in buckets.
![image info](./data/usecases_for_bucket_creation.jpg)

### Below use-case talks how multiple clients takes items and update. Also, it talks about challenges
![image info](./data/usecase_multi_client_processing.jpg)

### Below use-case talks about error conditions which can happen in this system
![image info](./data/usecase_error_Conditons_handling.jpg)

### Below use-case captures various software exceptions
![image info](./data/usecases_for_software_exceptions.jpg)

