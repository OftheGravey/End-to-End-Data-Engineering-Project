* Implement Flink checkpoints
* Remove the direct streaming sink from dimensions and facts to a batch job that pulls from the Kafka topics. This will allow for error correcting some of the fact to dimension joins, as temporal joins are not 100% reliable. 
* Add circuit breakers to allow upstream jobs to handle downstream job failures.
* These Flink jobs and their watermarks can lead to lost historical events if replayed. The watermark strategies are not currently adjustable to allow for more generous options when replaying data.
* Implement Dagster pipes or a similar solution to pass Flink job logs back to Dagster to centralize stream management.
* Add additional DAGs/Tools to assist with data stream user-friendliness. I.e. ability to close all Flink jobs