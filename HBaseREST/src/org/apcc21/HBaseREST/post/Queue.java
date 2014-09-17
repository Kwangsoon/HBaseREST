package org.apcc21.HBaseREST.post;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Queue extends ConcurrentHashMap{
	ConcurrentHashMap<String, String> al = null;
	Queue(){
		 al = new ConcurrentHashMap<String, String>();
	}
	
	public void enqueue(String row, String element){
		al.put(row, element);
		System.out.println("Inserting " + row  + "...");
	}
	
	public String dequeue(String row){
		String element = new String(al.get(row));
		System.out.println("Deleting " + row + "...");
		al.remove(row); //??
		//al.remove(al.size()); //??
		return element;
	}
	
	public int size(){
		return al.size();
	}
}
