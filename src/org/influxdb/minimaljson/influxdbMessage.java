package org.influxdb.minimaljson;


import java.util.Arrays;
import java.util.List;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public class influxdbMessage {
	public String influxDbName = "flumeDB";
	public String influxSeriesName = "test";
	public String[] influxDbColumns = {"time"};
	public String influxdbUrl;
	public String response;
	public JsonArray jsonBody = new JsonArray();
	
	public void setSeriesName(String seriesName){
		this.influxSeriesName = seriesName.isEmpty() ? this.influxSeriesName : seriesName;
	}
	
	public void initializeThis(Object object, Long pointTime) {
		JsonObject dataPoint = new JsonObject();
		dataPoint = JsonObject.readFrom(object.toString());
		Object[] influxDbPointVals = {"time"};
		int i = 0;
		int bLen = dataPoint.size();
		
		
		// if pointTime is null, then let influx create timestamp
		if (pointTime != null) {
			int aLen = influxDbColumns.length;
			
		
			influxDbColumns = new String[aLen+bLen];
			influxDbPointVals = new Object[aLen+bLen];
			
		
			influxDbColumns[0] = "\"time\"";
			influxDbPointVals[0] = pointTime;
			i=1;
		} else {
			influxDbColumns = new String[bLen];
			influxDbPointVals = new Object[bLen];
		}
		
		for(String dbCol : dataPoint.names()) {
			influxDbColumns[i]="\""+dbCol+"\"";
			influxDbPointVals[i]=dataPoint.get(dbCol).toString();
			i++;
		}

		JsonObject seriesWriteObject = new JsonObject().add("name", this.influxSeriesName);

		String dbColumnsArray = Arrays.toString(this.influxDbColumns);

		JsonArray dbColumnsJson = JsonArray.readFrom(dbColumnsArray);
		seriesWriteObject.add("columns", dbColumnsJson);

		JsonArray pointArray = new JsonArray();
		pointArray.add(JsonArray.readFrom(Arrays.toString(influxDbPointVals)));
		seriesWriteObject.add("points", pointArray);
		

		this.jsonBody.add(seriesWriteObject);

	}
	
	public influxdbMessage(Object object, Long pointTime) {
		initializeThis(object,pointTime);	
	}
	
	public influxdbMessage(JsonObject eventPoint) {
		initializeThis(eventPoint,null);
		
	}
	
	public influxdbMessage(JsonObject influxEventData, String timestampField) {
		
		if (influxEventData.names().contains(timestampField)) {
			Long time = influxEventData.get(timestampField).asLong();
			influxEventData.remove(timestampField);
			initializeThis(influxEventData,time);
		} else {
			System.out.println("Unable to locate timestamp field \"" + timestampField +".\" Using automatically generated timestamp from InfluxDB");
			initializeThis(influxEventData,null);
		}
			
	}

	public String[] getInfluxDbColumns() {

		String [] influxCols = this.influxDbColumns.clone();
		int i=0;
		for(String col : influxCols) {
			influxCols[i]=col.replace("\"", "");
			i++;
		}
		return influxCols;
	}

	public Object[] getInfluxDbPointVals() {
		List<JsonValue> influxData = this.jsonBody.asArray().get(0).asObject().get("points").asArray().values();
		Object[] dataArray = new Object[this.influxDbColumns.length];
		if (influxData.size() == 1 && influxData.get(0).isArray()) {
			influxData = (List<JsonValue>) influxData.get(0).asArray().values();
			int i=0;
			for(JsonValue obj : influxData) {
				if (obj.isString()) {
//					dataArray[i]=obj.asString().replace("\"", "");
					dataArray[i]=obj.asString();
				} else if (obj.isNumber()) {
					dataArray[i]=obj.asDouble();
				}
				i++;
			}
			
		}
		return dataArray;

	}

	public void setResponse(String response) {
		this.response=response;
	}

	public void addPoint(JsonObject eventPoint, String string, Object pointData) {
		// TODO Auto-generated method stub
		
	}

	public void addColumn(String newCol) {
		
		String[] oldColumns = this.influxDbColumns;
		int aLen = this.influxDbColumns.length;
		int bLen = 1;
		
		this.influxDbColumns = new String[aLen+bLen];
		
		int i = aLen + bLen;
		
		influxDbColumns[i-1] = newCol;
		
		i=0;
		for(String dbCol : oldColumns) {
			this.influxDbColumns[i]=dbCol;
			i++;
		}	
	}

	public void addColumns(String[] newCols) {
		// TODO Auto-generated method stub
		
		int numCols = newCols.length;
		
		for(int i=0;i<numCols;i++){
			addColumn(newCols[i]);
		}
		
	}

	public String getInfluxSeries() {
		// TODO Auto-generated method stub
		return this.influxSeriesName;
	}
}