package org.apcc21.HBaseREST.post;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;


public class Consumer implements Runnable{
	protected Queue queue;
	protected ArrayList<String> producedQueue;
	protected String url = "http://210.98.49.153:8080/climate/" + "row" + "/location";
	
	Consumer(Queue queue){
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.queue = queue;
	}
	
	public void run(){
		
		System.out.println("Start POST ....");
		while(true){
			if (queue.size() > 0){
				try{
					/*
					while(true){
						if (queue.size() > 0){
							
							String url = "http://210.98.49.153:8080/climate/" + "row" + queue.size() + "/location";
							System.out.println("URL : " + url);
		
							// Create HTTpClient Post
							HttpPost post = new HttpPost(url);
							
							// Create Entity
							String str = new String(queue.dequeue("row" + queue.size()).toString());
							StringEntity input = new StringEntity(str);
							input.setContentType("text/xml");
							
							// Update the Entity to Post
							post.setEntity(input);
		
							// Execute!
							HttpResponse response = client.execute(post);
							System.out.println("Response Code : " 
						                + response.getStatusLine().getStatusCode());
							
							System.out.println(queue.size());
		
							BufferedReader rd = new BufferedReader(
							        new InputStreamReader(response.getEntity().getContent()));
						 
							StringBuffer result = new StringBuffer();
							String line = "";
							while ((line = rd.readLine()) != null) {
								result.append(line);
							}
							
						}
					}
				*/
					
					/*
					Iterator<String> it = producedQueue.iterator();
					while(it.hasNext()){
						//System.out.println("URL : " + url);
						
						
						// Create HttpClient
						CloseableHttpClient client = HttpClientBuilder.create().build();
		
						// Create HTTpClient Post
						HttpPut post = new HttpPut(url);
						
						// Create Entity
						String element = it.next();
						StringEntity input = new StringEntity(element);
						input.setContentType("text/xml");
						
						// Update the Entity to Post
						post.setEntity(input);
						
						// Execute!
						CloseableHttpResponse response = client.execute(post);
						
						//System.out.println(element);
						//System.out.println("Response Code : " 
					    //            + response.getStatusLine().getStatusCode());
						
						// Release the connection
						post.releaseConnection();
						response.close();
						client.close();
						
						
					}
					*/
					/*
					for (Map.Entry<String, String> entry : queue.al.entrySet()){
						String url = "http://210.98.49.153:8080/climate/" + entry.getKey() + "/location";
						
						System.out.println("key : " + entry.getKey() + " " + "value : " + entry.getValue());
						
						// Create HttpClient
						CloseableHttpClient client = HttpClientBuilder.create().build();
		
						// Create HTTpClient Post
						HttpPut post = new HttpPut(url);
						
						// Create Entity
						String element = entry.getValue();
						StringEntity input = new StringEntity(element);
						input.setContentType("text/xml");
						
						// Update the Entity to Post
						post.setEntity(input);
						
						// Execute!
						CloseableHttpResponse response = client.execute(post);
						
						//System.out.println(element);
						System.out.println("Response Code : " 
					                + response.getStatusLine().getStatusCode());
						
						// Release the connection
						post.releaseConnection();
						response.close();
						client.close();
		
					}
					*/
					
					for (Map.Entry<String, String> entry : queue.al.entrySet()){
						String url = "http://210.98.49.153:8080/climate/" + entry.getKey() + "/location";
						
						System.out.println("value : " + entry.getKey());
							
						// Create HttpClient
						CloseableHttpClient client = HttpClientBuilder.create().build();
		
						// Create HTTpClient Post
						HttpPut post = new HttpPut(url);
						
						// Create Entity
						String element = entry.getValue();
						StringEntity input = new StringEntity(element);
						input.setContentType("text/xml");
						
						// Update the Entity to Post
						post.setEntity(input);
						
						// Execute!
						CloseableHttpResponse response = client.execute(post);
						
						//System.out.println(element);
						System.out.println("Response Code : " 
					                + response.getStatusLine().getStatusCode());
						
						// Release the connection
						post.releaseConnection();
						response.close();
						client.close();
						
						System.out.println("Deleting " + entry.getKey() + "...");
						queue.al.remove(entry.getKey());
					}
						
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	}
}
