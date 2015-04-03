 
package org.influxdb.minimaljson;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flume.EventDeliveryException;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;


public class Influxdb {
	private String DEFAULT_TIME_UNIT = "m";
	
	
	private String server;
	private int port;
	private String[] servers;
	private String database;
	private String username;
	private String password;
	private String urlBase;
	private String timeUnit = DEFAULT_TIME_UNIT;
    
	
	public Influxdb(String hosts, int port, String database, String username,
			String password, TimeUnit timeunit) {
		this.servers = hosts.split(" ");
		this.port= port;
		this.server = this.servers[0] + ":" + Integer.toString(this.port);
		
		this.database = database;
		this.username = username;
		this.password = password;
		
        
        try {
            this.urlBase = new String("http://" + this.server + "/db/");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        
    
	}

	public Influxdb(String hosts, int port, String database, String username,
			String password, String timeunit) {
		this.servers = hosts.split(" ");
		this.port= port;
		this.server = this.servers[0] + ":" + Integer.toString(this.port);
		
		this.database = database;
		this.username = username;
		this.password = password;
		this.timeUnit = timeunit;
        
        try {
            this.urlBase = new String("http://" + this.server + "/db/");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

	}
	public void writePoints(String series, String[] influxDbColumns,
			Object[] pointData) throws IOException {
		writePoints(this.database,series,influxDbColumns,pointData);
	}
	
	
	public void writePoints(String influxDbName, String series, String[] influxDbColumns,
			Object[] pointData) throws IOException {
		HttpURLConnection urlConn = null;
		URL url;
		OutputStream output = null;
        InputStream input = null;
        int hostCounter=0;
        boolean successfulPost=false;
        ArrayList<IOException> serverErrors = new ArrayList<IOException>(); 
        
        while (hostCounter<this.servers.length && successfulPost==false){
            // URL connection channel.
        	String parameters=influxDbName + "/series?u=" + this.username + "&p=" + this.password + "&time_precision=" + timeUnit;
        	
    		String influxdbUrlString = this.urlBase + parameters;
    		
        	url = new URL(influxdbUrlString);
            urlConn = (HttpURLConnection) url.openConnection();
        
        // Let the run-time system (RTS) know that we want input.
        urlConn.setDoInput (true);

        // Let the RTS know that we want to do output.
        urlConn.setDoOutput (true);

        // No caching, we want the real thing.
        urlConn.setUseCaches (false);
        
        

        // Construct the POST data.
        String content = null;
			content = influxJsonBody(series,influxDbColumns,pointData);
		    urlConn.setRequestMethod("POST");

            urlConn.setRequestProperty("Content-Type",
              "application/json");
            urlConn.setRequestProperty("Content-Length", ""+content.length());
            
            output = urlConn.getOutputStream();
            
        // Send the request data.
            output.write(content.getBytes("UTF-8"));
            output.flush();
            output.close();
        
        	if (urlConn.getResponseCode() == 200) {
        		input = urlConn.getInputStream();
        		successfulPost=true;
        		
        	} else {
        	     /* error from server */
        		input = urlConn.getErrorStream();
        		serverErrors.add(new IOException(urlConn.getResponseCode() + "\n " + urlConn.getResponseMessage() + "\n" + input.toString() + " on message: \n"+content));
        		this.server = this.servers[hostCounter] + ":" + Integer.toString(this.port);
        		hostCounter+=1;
                try {
                    this.urlBase = new String("http://" + this.server + "/db/");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
        	}
        }
            
        if (successfulPost==false){
        	for (int i=0;i<serverErrors.size();i++){
        		System.out.println(serverErrors.get(i).getMessage());
        	}
        	throw serverErrors.get(serverErrors.size()-1);
        }
        // Get response data.      
        	int charInt;
        	
            while ((charInt = input.read()) != -1) {
            	//TODO: logger for debugging
            	// System.out.println((char) charInt);
            }
            input.close ();
		
	}

	private String influxJsonBody(String series, String[] columns, Object[] pointData) {
		JsonArray influxJson = new JsonArray();
		JsonArray columnArray = new JsonArray();
		JsonArray dataArray = new JsonArray();
		
		series = series.replace(":", "-");
		
		for(String colName : columns) {
			columnArray.add(colName);
		}
		
		for(Object dataVal : pointData) {
			if (dataVal != null) {
				if (dataVal.getClass() == int.class || dataVal.getClass() == Integer.class){
					dataArray.add((Integer) dataVal);
				} else
					if (dataVal.getClass() == Long.class || dataVal.getClass() == long.class){
						dataArray.add((Long) dataVal);
					} else
						if (dataVal.getClass() == Double.class || dataVal.getClass() == double.class){
							dataArray.add((Double) dataVal);
						} else if (dataVal.getClass() == String.class)
						{	
							String dataValue = String.valueOf(dataVal).replace("\"", "");
							dataArray.add(dataValue);
						} else if (dataVal.getClass() == float.class || dataVal.getClass() == Float.class)
						{	
							dataArray.add((Float) dataVal);
							
						}else
						{
							String dataValue = String.valueOf(dataVal).replace("\"", "");
							dataArray.add(dataValue);
						}
			}
			
		}
		influxJson.add(new JsonObject().add("name", series).add("columns", columnArray).add("points", new JsonArray().add(dataArray)));
		return influxJson.toString();
	}

	public void writePoints(List<influxdbMessage> influxMessages) throws IOException {
		for (influxdbMessage influxMsg : influxMessages) {
			writePoints(influxMsg.getInfluxSeries(),influxMsg.getInfluxDbColumns(),influxMsg.getInfluxDbPointVals());
		}
		
	}

	

}
