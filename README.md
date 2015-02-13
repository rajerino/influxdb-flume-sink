# influxdb-flume-sink
Configurable Flume Sink that processes flume JSON-formatted events and sends them to an InfluxDB series using the InfluxDB HTTP API.

[InfluxDB](http://www.influxdb.com)

[Flume-NG](https://flume.apache.org/)

## Dependencies
* minimal-json (com.eclipsesource.json at https://github.com/ralfstx/minimal-json)
* flume-ng-{configuration,core,sdk} (org.apache.flume at https://github.com/apache/flume)
* slf4j-api (org.slf4j.Logger* at https://github.com/qos-ch/slf4j)

## Compiling/Running
The relevant class or jar files for custom Flume sinks can be placed in the Flume classpath,
or the sinks can be compiled along with Flume Source.

## Configuring Flume
Set the type of the sink to the class name, e.g.:

`agent1.sinks.influxDBSink1.type = org.apache.flume.sink.api.InfluxDBSink`

### Configurable Sink Properties 

Required properties are in **bold**.

Property Name | Default    | Description
--- | --- | ---
**channel**| - | 
**type**| -	   | Type name/class-- needs to be org.apache.flume.sink.api.InfluxDBSink
host| localhost  | The hostname or IP of InfluxDB node
hosts| - | Space-separated hostnames/IPs of InfluxDB nodes to try in turn in case of failure
port| 8086 | Port InfluxDB is listening for commands to its HTTP API
username| root | InfluxDB username with write permissions on the database specified in the *database* property
password| root | InfluxDB password corresponding to *username* property
database| flumetest | Database in which the InfluxDB series specified in *seriesName* lives
seriesName| - | Name of InfluxDB series to which this sink writes its messages.
txnEventMax | 100 | Max number of events to batch from the Flume channel for processing 
timestampField | - | If passing a timestamp for the datapoint, specify its level-0 field name in the JSON dict here
fieldsToExclude | - | Do not send these space-separated fields from level-0 of the incoming JSON messages to the InfluxDB series
timeUnit | ms | If sending the timestamp with the incoming message on the *timestamp* field, these are the units (see InfluxDB docs)
dataField | - | If data is in a level-1 dictionary in the message JSON, specify the field name for it here
prependDataField | - | If a *dataField* is specified, this will prepend the string specified here to all columns in the *dataField* datapoint
