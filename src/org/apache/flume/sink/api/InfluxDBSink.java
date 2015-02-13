package org.apache.flume.sink.api;


import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.Sink.Status;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.instrumentation.SinkCounter;
import org.influxdb.minimaljson.Influxdb;
import org.influxdb.minimaljson.influxdbMessage;


public class InfluxDBSink extends AbstractSink implements Configurable {
	private String host = "localhost";
	private String hosts = "";
	private int port = 8086;
	private String username = "root";
	private String password = "root";
	private String database = "flumetest";
	private Influxdb influxdb = null;
	private String fieldsToExclude = ""; // specify list of keys to exclude when passing data to influxDB, separated by spaces
	private String dataField = ""; // specify nested data dictionary's key here 
	private String timestampField = ""; // field from which to read timestamp
	private String prependDataField = "false";
	private String messageType = ""; //defaults to JSON object
	private Context context;
	private String seriesName="";
	private long txnEventMax=100;
	private String timeUnit="ms";
	private SinkCounter sinkCounter;

	private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBSink.class);
	@Override
	public void configure(Context context) {
		this.context = context;

		this.host = context.getString("host", this.host);
		this.hosts = context.getString("hosts", this.hosts);
		this.port = context.getInteger("port", this.port);
		this.username = context.getString("username", this.username);
		this.password = context.getString("password", this.password);
		this.database = context.getString("database", this.database);
		this.fieldsToExclude = context.getString("fieldsToExclude", fieldsToExclude);
		this.txnEventMax = context.getLong("txnEventMax", this.txnEventMax);
		this.dataField = context.getString("dataField", dataField);
		this.timestampField = context.getString("timestampField", timestampField);
		this.prependDataField = context.getString("prependDataFieldNames", prependDataField);
		this.messageType = context.getString("messageType", this.messageType);
		this.seriesName = context.getString("seriesName", this.seriesName);
		this.timeUnit = context.getString("timeUnit", this.timeUnit);

		if (this.hosts.isEmpty()) {
			LOGGER.info("Configured InfluxDB Sink to host {} .", this.host);
		} else {
			if (this.hosts.split(" ").length>1){
				LOGGER.info("Configured InfluxDB Sink to cluster hosts {} .", this.hosts);
			} else {
				LOGGER.info("Configured InfluxDB Sink to host {} .", this.hosts);
			}
			
		}
		LOGGER.info("Configured InfluxDB Sink messageType to {} .", this.messageType.isEmpty() ? "default (JSON object)" : this.messageType);

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

		//TODO : validation

	}

	@Override
	public void start() {
		LOGGER.info("Starting InfluxDB Sink {} ...", this);
		// Initialize the connection to InfluxDB that
		// this Sink will forward Events to ..
		try {
			this.influxdb = new Influxdb(this.hosts.isEmpty() ? this.host : this.hosts, this.port, this.database, this.username, this.password, this.timeUnit);
			sinkCounter.incrementConnectionCreatedCount();
		} catch (Exception e) {
			sinkCounter.incrementConnectionFailedCount();
			e.printStackTrace();
		}
		LOGGER.info("InfluxDB Sink {} started.", this);
		sinkCounter.start();
	}

	private TimeUnit getTimeUnit(String time) {
		TimeUnit timeUnit;
		if (time == "m"){
			timeUnit = TimeUnit.MILLISECONDS;
		} else if (time == "s") {
			timeUnit = TimeUnit.SECONDS;
		} else if (time == "u") {
			timeUnit = TimeUnit.MICROSECONDS;
		} else {
			timeUnit = TimeUnit.MILLISECONDS;
		}
		return timeUnit;
	}

	@Override
	public void stop () {
		LOGGER.info("Stopping InfluxDB Sink {} ...", this);
		// Disconnect from the external respository and do any
		// additional cleanup (e.g. releasing resources or nulling-out
		// field values) ..
		sinkCounter.incrementConnectionClosedCount();
		sinkCounter.stop();
		
		super.stop();
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;

		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();

		txn.begin();

		try {
			Event event = null;
			int txnEventCount = 0;
			int attempts = 0;
			for (txnEventCount = 0; txnEventCount < txnEventMax; txnEventCount++) {
				event = ch.take();
			 if (event != null) {
				
				attempts++;
				
				String[] pointColumns;
				Object[] pointData;
				try {
					influxdbFlumeHandler influxHelper = new influxdbFlumeHandler(event,
						fieldsToExclude,dataField,timestampField,messageType,
						Boolean.valueOf(this.prependDataField));

					influxdbMessage influxMessage = influxHelper.getInfluxMessage();
					influxMessage.setSeriesName(this.seriesName);
				
					pointColumns = influxMessage.getInfluxDbColumns();
					pointData = influxMessage.getInfluxDbPointVals();
					
					String seriesName = influxMessage.getInfluxSeries();
				

					this.influxdb.writePoints(database, seriesName, pointColumns, pointData);
					LOGGER.info("Recorded to InfluxDB series \"{}\" in database \"{}\" ...", seriesName, this.database);
				} catch (Exception e) {
					txn.rollback();
					status=status.BACKOFF;
					byte[] messageBytes = event.getBody();
					String messageString = new String(messageBytes);
					LOGGER.error("InfluxDB Sink " + getName() + " exception: Unsuccessful event handling from channel " + ch.getName()
									+ ". Exception follows.", e);
					throw e;
				}
				
				sinkCounter.incrementConnectionCreatedCount();
			} else {
				LOGGER.error("NULL Flume event on " + ch.getName());
				break;
			}
		}
			sinkCounter.addToEventDrainAttemptCount(attempts);
			if (txnEventCount == 0) {
				sinkCounter.incrementBatchEmptyCount();
			} else if (txnEventCount == txnEventMax) {
				sinkCounter.incrementBatchCompleteCount();
			} else {
				sinkCounter.incrementBatchUnderflowCount();
			}

			txn.commit();
			if (txnEventCount > 0) {
				sinkCounter.addToEventDrainSuccessCount(txnEventCount);
			}
			
			if(event == null) {
				status = Status.BACKOFF;
			}

			return status;
		}
		
		catch (IOException e){
			txn.rollback();
			LOGGER.error("InfluxDB Sink " + getName() + ": Unable to process event from channel " + ch.getName()
					+ ". Exception follows.", e);
			// Log exception, handle individual exceptions as needed

			//status = Status.BACKOFF;

			// re-throw all Errors
			
			throw new EventDeliveryException(e);
			
		}
		catch (Exception e) {
			sinkCounter.incrementConnectionFailedCount();
			txn.rollback();
			LOGGER.error("InfluxDB Sink " + getName() + ": Exception while writing ", e);
			e.printStackTrace();
			return Status.BACKOFF;

		}
		 finally {
			txn.close();
		}
	}
//	return status;

}
