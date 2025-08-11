* Implement Flink checkpoints
* Add circuit breakers to allow upstream jobs to handle downstream job failures
* Implement Dagster pipes or a similar solution to pass Flink job logs back to Dagster to centralize stream management.
* Add additional DAGs/Tools to assist with data stream user-friendliness. I.e. ability to close all Flink jobs