package org.apache.flume.sink.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flume.Event;
import org.influxdb.minimaljson.influxdbMessage;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonObject.Member;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonValue;
import com.eclipsesource.json.ParseException;

public class influxdbFlumeHandler {
	public List<String[]> pointColumns = new ArrayList<String[]>();
	public List<Object[]> pointData = new ArrayList<Object[]>();
	public influxdbMessage influxMessage;
	public List<String> fieldsToExclude = new ArrayList<String>(); 

	private String timestampField = "";
	public boolean prependColumnNames=false;
	private String messageType="";
	public String seriesName="";
	private String columns;
	private String points;
	private boolean pointsList=false;
	private List<influxdbMessage> influxMessages = new ArrayList<influxdbMessage>();
	
	public void setPrependOption (boolean prepend) {
		this.prependColumnNames = prepend;
	}
	
	
	public void init (String messageType) {

		if (messageType.equalsIgnoreCase("influxdbHTTP")) {
			this.timestampField = "time";
			this.columns = "columns";
			this.points = "points";
			this.pointsList = true;
			this.prependColumnNames = false;
			
		}

	}
	
	public void newEvent (Event event) {
		byte[] eventMessageBytes = event.getBody();
		String eventMessageString = new String(eventMessageBytes);
		try {
			JsonValue.readFrom(eventMessageString);			
			handleEvent(eventMessageString);
		} catch (Exception e) {

			e.printStackTrace();
			return;
		}
	}
	
	public void handleEvent (String messageString) {

		JsonValue eventMessage;
		try {
			eventMessage =  this.messageType.equals("influxdbHTTP") ? JsonValue.readFrom(messageString).asArray() : JsonObject.readFrom(messageString);
		} catch (ParseException e) {
			
			eventMessage = new JsonObject();
		}
		
					
		if (eventMessage.isArray()){
			for (JsonValue influxSeries : eventMessage.asArray()){
				handleInfluxSeries(influxSeries.asObject());
			}
		} else if (eventMessage.isObject()) {
			handleInfluxSeries(eventMessage.asObject());
		}
		
	}

	private void handleInfluxSeries(JsonObject eventMessage) {
		JsonObject influxSeriesData = new JsonObject();
		// retrieve data from eventMessage into local JsonObject
		for (Member jval : eventMessage) {
			String jvalName = jval.getName();

			if (!this.fieldsToExclude.contains(jvalName)) {
				// if the field is meant to be excluded, skip it.
				// otherwise, send along the parameter/key
				if (jval.getValue().isString()) {
					if (jval.getValue().asString()!="" && !jval.getValue().isNull()) {
						influxSeriesData.add(jval.getName(), jval.getValue());
					}
				}
				else {
					if (!jval.getValue().isNull()) {
						influxSeriesData.add(jval.getName(), jval.getValue());
					}
				}

			} else {

			}

		}


		if(!this.columns.isEmpty()) {
			// retrieve as a JsonValue the value of property "data" from event message
			JsonValue eventPoint = eventMessage.get(this.columns);

			try {
				if (eventPoint.isObject()) { // likely points and columns are one JsonObject
					for (Member jval : eventPoint.asObject()) {
						// so just copy over the keys and values
						influxSeriesData.add((prependColumnNames ? this.columns + "-" : "") + jval.getName(), jval.getValue());
					}
				} else if (eventPoint.isArray()) { // points and columns are separate JsonArrays
					JsonArray pointCols=eventPoint.asArray(); // so copy keys from columns array
					JsonArray pointVals=eventMessage.get(this.points).asArray(); // and values from points array
					if (this.pointsList==true) {
						influxSeriesData.add("columns", pointCols);
						influxSeriesData.add("points", new JsonArray());
						for (JsonValue currentPoint: pointVals) {
							influxSeriesData.get("points").asArray().add(currentPoint.asArray());
						}
					} else
					{
						for (Integer i = 0; i < pointVals.size(); i++){
							influxSeriesData.add((prependColumnNames ? this.columns + "-" : "") + pointCols.get(i), pointVals.get(i));
						}
					}


				}
			} catch (ParseException e) {
				System.out.println("Column field " + this.columns + " is type " + eventPoint.getClass() + " while point field " + this.points + " is type " + eventMessage.get(this.points).getClass());
			}

		}

		this.influxMessages.add(newInfluxdbMessage(influxSeriesData));
		Integer currentMessagePosition= this.influxMessages.size() -1;
		this.pointColumns.add(this.influxMessages.get(currentMessagePosition).getInfluxDbColumns());
		this.pointData.add(this.influxMessages.get(currentMessagePosition).getInfluxDbPointVals());
		
	}


	private influxdbMessage newInfluxdbMessage(JsonObject influxEventData) {
		if (this.messageType.equals("influxdbHTTP")){
			influxdbMessage influxMessageFinal = new influxdbMessage();
			influxMessageFinal.readFromSeriesWriteObject(influxEventData);
			return influxMessageFinal;
		} else if (!this.seriesName.isEmpty()) {
			return new influxdbMessage(influxEventData, this.timestampField, this.seriesName);
		} else {
			return new influxdbMessage(influxEventData, this.timestampField);
		}
		
	}


	public influxdbFlumeHandler(String fieldsToExcludeString, String dataField, String timestampField,String messageType, String seriesName,boolean prependOption) {

		this.fieldsToExclude = fieldsToExcludeString.isEmpty() ? this.fieldsToExclude : Arrays.asList(fieldsToExcludeString.split(" "));
		this.timestampField = timestampField;
		this.columns = dataField;
		this.messageType = messageType;
		this.seriesName = seriesName;
//		System.out.println("Message Type: "+messageType);
		this.init(messageType);
		setPrependOption(prependOption);
		
	}

	public influxdbMessage getInfluxMessage() {

		return this.influxMessage;
	}

	public String getSeriesName() {

		return this.influxMessage.getInfluxSeries();
	}


	public List<influxdbMessage> getInfluxMessages() {
		// TODO Auto-generated method stub
		return this.influxMessages;
	}
	

}