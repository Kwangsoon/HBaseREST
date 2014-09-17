package org.apcc21.HBaseREST.get;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class HBaseHttpGet {
	String url = "";
	
	HBaseHttpGet(String url){
		this.url = url;
	}
	
	public String get() throws ClientProtocolException, IOException{
		String result = "";
		HttpClient client = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(url);
		httpget.setHeader("Accept", "application/json");
		HttpResponse response = client.execute(httpget);
		
		BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
		
		String line = "";
		StringBuffer lineBuffer = new StringBuffer();
		while ((line = rd.readLine()) != null) {
			//textView.append(line);
			//System.out.println(line);
			lineBuffer.append(line);
			//JsonReader jr = new JsonReader(line);
			//gson(line);
		} 	
		result = lineBuffer.toString();
		return result;
	}
}
