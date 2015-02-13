 
package org.influxdb.minimaljson;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;


import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;


public class Influxdb {
	private String DEFAULT_TIME_UNIT = "m";
	
	
	private String server;
	private String database;
	private String username;
	private String password;
	private String urlBase;
	private String timeUnit = DEFAULT_TIME_UNIT;
    
	
	public Influxdb(String host, int port, String database, String username,
			String password, TimeUnit timeunit) {
		this.server = host + ":" + Integer.toString(port);
		this.database = database;
		this.username = username;
		this.password = password;
		
        
        try {
            this.urlBase = new String("http://" + server + "/db/");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        
    
	}

	public Influxdb(String host, int port, String database, String username,
			String password, String timeunit) {
		this.server = host + ":" + Integer.toString(port);
		this.database = database;
		this.username = username;
		this.password = password;
		this.timeUnit = timeunit;
        
        try {
            this.urlBase = new String("http://" + server + "/db/");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

	}

	public void writePoints(String influxDbName, String series, String[] influxDbColumns,
			Object[] pointData) throws IOException {
		HttpURLConnection urlConn = null;
		URL url;
		OutputStream output = null;
        InputStream input = null;
        
        
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
            //urlConn.setRequestProperty("Content-Length", ""+content.length);            
            output = urlConn.getOutputStream();

        // Send the request data.
            output.write(content.getBytes("UTF-8"));
            output.flush();
            output.close();
        
        	if (urlConn.getResponseCode() == 200) {
        		input = urlConn.getInputStream();
        		
        	} else {
        	     /* error from server */
        		input = urlConn.getErrorStream();
        		throw new IOException(urlConn.getResponseCode() + "\n " + urlConn.getResponseMessage() + "\n" + input.toString() + " on message: \n"+content);
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
				if (dataVal.getClass() == int.class){
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
						} else if (dataVal.getClass() == float.class)
						{	
							dataArray.add((Double) dataVal);
							
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

	

}
