package org.apcc21.HBaseREST.post;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public class HBaseWriter {
	public String filePath = "";
	public int numberOfThread;
	public Queue queue;
	public Thread produceThread;
	public Thread [] consumerThread;
	
	/*
	 * Constructor
	 */
	HBaseWriter(String filePath, int numberOfThread){
		this.filePath = filePath;
		this.numberOfThread = numberOfThread;
		consumerThread = new Thread[numberOfThread];
		queue = new Queue();
	}
	
	public void startThreads(){
		produceThread = new Thread(new Producer(queue, filePath), "Producer");
		produceThread.start();
		
		for (int i = 0; i < numberOfThread; ++i){
			consumerThread[i] = new Thread(new Consumer(queue), "Consumer" + i);
			consumerThread[i].start();
		}
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String filePath = "C:\\Users\\user\\workspace\\NetcdFileTest\\data\\air.1948.nc";
		int numberOfThread = 1;
		HBaseWriter hbaseWriter = new HBaseWriter(filePath, numberOfThread);
		hbaseWriter.startThreads();
	}
}