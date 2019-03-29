# Event Session Joiner

* This is a Spark structured streaming job that is capable of (inner) joining any number streams with any kind of fields on any kind of conditions and output any kind of fields.

* The input streams must be coming in as JSON objects.

* The resulting output stream is serialized as JSON.

## Configuration

The job is configured with the following parameters in the `config.yml` file.
The config file needs to be uploaded into HDFS and the path needs to be specified as a command line argument at the job submission time.
An example config file can be found here `/src/main/resources/config.yml`.

* `kafka_bootstrap_servers`: quorum of (recommended at least 3) Kafka brokers and their ports; String

* `kafka_security_protocol`: protocol to use to connect to the Kafka brokers; String

* `kafka_input_streams`: A list of definitions of streams that shall be joined; List

  * `name`: The name of the stream. It is used to uniquely identify columns with the same name in the `join_condition` and `output_columns`; String

  * `topic`: Kafka topic to read events from; String

  * `delay_threshold`: The delay an event is kept in memory before it gets flushed, in case events don't arrive at the same time; String

  * `columns`: Input columns to use from the incoming JSON object. These columns need to be available in the incoming JSON object; List

  * `join_condition`: The condition on which the stream joins with all of the streams above itself. Thus, the first one in the list does not have a `join_condition`. In the `join_condition`: all variables defined in the stream and all previous streams are available; String

* `output_columns`: Specify all output columns you want to be using in the output JSON object; List

* `output_format`: This should always be `kafka`; String

* `kafka_output_topic`: Specify the Kafka topic, you want the joined stream to be ingested to; String

* `checkpointLocation`: Specify the checkpoint location. The checkpoint keeps track of the state so the job can be restarted without losing data. This is a path in HDFS; String

## Build

Just run Maven:

```bash
mvn clean package
```

## Deployment

* This app was written to run on `HDP 2.6.5` using `Spark 2.3.x`

* Deploy the job jar to the Metron REST node.

* Adapt the script `submit-event-session-joiner.sh` to your environment (jar location, filename, keytab location, domain name)

* To submit the job execute the `submit-event-session-joiner.sh` script from the Metron REST node.

* Use YARN to manage the lifecycle (kill, monitor,...) the job.


## ToDos

* Add unit tests
* Modularize
* Feature: specify datatypes
