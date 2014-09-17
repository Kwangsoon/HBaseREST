package org.apcc21.HBaseREST.post;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;

import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayShort;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class Producer implements Runnable{
	protected String filePath;
	protected ClimateData climateData;
	protected Queue queue;
	
	/*
	 * Constructor
	 */
	Producer(Queue queue, String filePath){
		this.filePath = filePath;
		this.climateData = new ClimateData(filePath);
		this.queue = queue;
	}
	
	/*
	 * Extracting data from netCDF file
	 * @see java.lang.Runnable#run()
	 */
	public void run(){
	     climateData.getClimateData();
	     
	     long cnt = 0;
	     StringBuffer qr = new StringBuffer();
		 qr.append("<CellSet>");
	     for (int i = 0 ; i < climateData.getTimeSize(); ++i)
	    	 for (int j = 0; j < climateData.getLevelSize(); ++j)
	    		 for (int k = 0; k < climateData.getLatSize(); ++k)
	    			 for (int l = 0; l < climateData.getLonSize(); ++l){
	    				 //System.out.println("i : " + i +" j : " + j +" K : " + k + " l : " + l);
	    				 ++cnt;
	    				 StringBuffer bufferStr = queryStr(cnt, climateData.getTimeArray().get(i), climateData.getLevelArray().get(j), climateData.getLatArray().get(k), climateData.getLonArray().get(l), climateData.getAirArray().get(i, j, k, l));
	    				 qr.append(bufferStr);
	    				 
	    				 if (cnt%5000 == 0){
	    					 //System.out.println("cnt : " + cnt);
	    					 qr.append("</CellSet>");
	    					 queue.enqueue("row" + (cnt - 4999), qr.toString());
	    					 qr = new StringBuffer();
	    					 qr.append("<CellSet>");
	    				 }
	    			 }
	     
	     qr.append("</CellSet>");
		 queue.enqueue("row" + (cnt - 4999), qr.toString());
	     
		 
	     // The file is closed no matter what by putting inside a try/catch block.
	     System.out.println("*** SUCCESS reading example file "+ filePath);
	}
	
	
	/*
	 * Args : size, time, level, latitude, longitude, air
	 * Constructing a insertion query of HBase
	 */
	public void query(Long size, Double time, Float level, Float latitude, Float longitude, Short air){

		Base64 base64 = new Base64();
		byte[] encodedBytes = base64.encodeBase64(("row" + size.toString()).getBytes());
		String row64 = new String (encodedBytes);
		
		byte[] encodedTimeBytes = base64.encodeBase64((time.toString()).getBytes());
		String time64 = new String (encodedTimeBytes);
		String timeCellStr = "<Cell column=\"" + "bG9jYXRpb246dGltZQ==" + "\">" + time64 + "</Cell>";

		byte[] encodedLevelBytes = base64.encodeBase64((level.toString()).getBytes());
		String level64 = new String (encodedLevelBytes);
		String levelCellStr = "<Cell column=\"" + "bG9jYXRpb246bGV2ZWw=" + "\">" + level64 + "</Cell>";
		
		byte[] encodedLatitudeBytes = base64.encodeBase64((latitude.toString()).getBytes());
		String latitude64 = new String (encodedLatitudeBytes);
		String latitudeCellStr = "<Cell column=\"" + "bG9jYXRpb246bGF0aXR1ZGU=" + "\">" + latitude64 + "</Cell>";
		
		byte[] encodedLongitudeBytes = base64.encodeBase64((longitude.toString()).getBytes());
		String longitude64 = new String (encodedLongitudeBytes);
		String longitudeCellStr = "<Cell column=\"" + "bG9jYXRpb246bG9uZ2l0dWRl" + "\">" + longitude64 + "</Cell>";
		
		byte[] encodedAirBytes = base64.encodeBase64((air.toString()).getBytes());
		String air64 = new String (encodedAirBytes);
		String airCellStr = "<Cell column=\"" + "bG9jYXRpb246YWly" + "\">" + air64 + "</Cell>";
		
		/*
		String row64 = encodingToBase64("row" + size.toString());
		String timeCellStr = encodingToBase64(time.toString());
		String levelCellStr = encodingToBase64(level.toString());
		String latitudeCellStr = encodingToBase64(latitude.toString());
		String longitudeCellStr = encodingToBase64(longitude.toString());
		String airCellStr = encodingToBase64(air.toString());
		*/
		
		StringBuffer qr = new StringBuffer();
		qr.append("<CellSet>");
		qr.append("<Row key=\"");
		qr.append(row64);
		qr.append("\">");
		qr.append(timeCellStr);
		qr.append(levelCellStr);
		qr.append(latitudeCellStr);
		qr.append(longitudeCellStr);
		qr.append(airCellStr);
		qr.append("</Row>");
		qr.append("</CellSet>");
		
		//System.out.println(qr);
		queue.enqueue("row" + size.toString(), qr.toString());

		//System.out.println("Enqueue : " + qr);
		//producedQueue.add(qr.toString());
	}
	
	
	public StringBuffer queryStr(Long size, Double time, Float level, Float latitude, Float longitude, Short air){

		Base64 base64 = new Base64();
		byte[] encodedBytes = base64.encodeBase64(("row" + size.toString()).getBytes());
		String row64 = new String (encodedBytes);
		
		byte[] encodedTimeBytes = base64.encodeBase64((time.toString()).getBytes());
		String time64 = new String (encodedTimeBytes);
		String timeCellStr = "<Cell column=\"" + "bG9jYXRpb246dGltZQ==" + "\">" + time64 + "</Cell>";

		byte[] encodedLevelBytes = base64.encodeBase64((level.toString()).getBytes());
		String level64 = new String (encodedLevelBytes);
		String levelCellStr = "<Cell column=\"" + "bG9jYXRpb246bGV2ZWw=" + "\">" + level64 + "</Cell>";
		
		byte[] encodedLatitudeBytes = base64.encodeBase64((latitude.toString()).getBytes());
		String latitude64 = new String (encodedLatitudeBytes);
		String latitudeCellStr = "<Cell column=\"" + "bG9jYXRpb246bGF0aXR1ZGU=" + "\">" + latitude64 + "</Cell>";
		
		byte[] encodedLongitudeBytes = base64.encodeBase64((longitude.toString()).getBytes());
		String longitude64 = new String (encodedLongitudeBytes);
		String longitudeCellStr = "<Cell column=\"" + "bG9jYXRpb246bG9uZ2l0dWRl" + "\">" + longitude64 + "</Cell>";
		
		byte[] encodedAirBytes = base64.encodeBase64((air.toString()).getBytes());
		String air64 = new String (encodedAirBytes);
		String airCellStr = "<Cell column=\"" + "bG9jYXRpb246YWly" + "\">" + air64 + "</Cell>";
		
		StringBuffer qr = new StringBuffer();
		qr.append("<Row key=\"");
		qr.append(row64);
		qr.append("\">");
		qr.append(timeCellStr);
		qr.append(levelCellStr);
		qr.append(latitudeCellStr);
		qr.append(longitudeCellStr);
		qr.append(airCellStr);
		qr.append("</Row>");
		
		return qr;
		
	}
	
	/*
	 * 
	 */
	public String encodingToBase64(String str){
		Base64 base64 = new Base64();
		byte[] encodedBytes = base64.encodeBase64((str).getBytes());
		String row64 = new String (encodedBytes);
		
		return row64;
	}
}
