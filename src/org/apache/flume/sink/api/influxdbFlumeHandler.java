package org.apache.flume.sink.api;

import java.util.Arrays;

import org.apache.flume.Event;
import org.influxdb.minimaljson.influxdbMessage;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonObject.Member;
import com.eclipsesource.json.ParseException;

public class influxdbFlumeHandler {
	public String[] pointColumns;
	public Object[] pointData;
	public influxdbMessage influxMessage;
	public String[] fieldsToExclude={""}; 
	private String dataField;
	private String timestampField = "";
	public boolean prependDataField=false;
	private String messageType="";
	
	public void setPrependOption (boolean prepend) {
		this.prependDataField = prepend;
	}
	
	public void init (Event event) {
		byte[] messageBytes = event.getBody();
		String messageString = new String(messageBytes);
		JsonObject eventMessage ;
		try {
			eventMessage = JsonObject.readFrom(messageString);
		} catch (ParseException e) {
			eventMessage = new JsonObject();
		}
		JsonObject influxEventData = new JsonObject();
		
			
			// retrieve data from eventMessage into local JsonObject
			for (Member jval : eventMessage) {
				String jvalName = jval.getName();

				if (!Arrays.asList(fieldsToExclude).contains(jvalName)) {
					// otherwise send along the parameter/key
					if (jval.getValue().isString()) {
						if (jval.getValue().asString()!="" && !jval.getValue().isNull()) {
							influxEventData.add(jval.getName(), jval.getValue());
						}
					}
					else {
						if (!jval.getValue().isNull()) {
							influxEventData.add(jval.getName(), jval.getValue());
						}
					}
						
				} else {

				}

			}

			// retrieve as a JsonObject the value of property "data" from event message  
			if(!dataField.isEmpty()) {
				try {
					JsonObject eventPoint = eventMessage.get(dataField).asObject();
					for (Member jval : eventPoint) {
						influxEventData.add((prependDataField ? dataField + "-" : "") + jval.getName(), jval.getValue());
					}
				} catch (ParseException e) {
					System.out.println("Data field " + dataField + " is not an object.");
				}
				
			}
		
		this.influxMessage = new influxdbMessage(influxEventData, timestampField);
		this.pointColumns = influxMessage.getInfluxDbColumns();
		this.pointData = influxMessage.getInfluxDbPointVals();
	}

	public influxdbFlumeHandler(Event event,
			String fieldsToExclude, String dataField, String timestampField,String messageType, boolean prependOption) {

		this.fieldsToExclude = fieldsToExclude.isEmpty() ? this.fieldsToExclude : fieldsToExclude.split(" ");
		this.timestampField = timestampField;
		this.dataField = dataField;
		this.messageType = messageType;
		setPrependOption(prependOption);
		init(event);
		
	}

	public influxdbMessage getInfluxMessage() {

		return this.influxMessage;
	}
	

}