package org.apcc21.HBaseREST.post;

import java.io.IOException;

import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayShort;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class ClimateData {
	protected String filename;
	protected NetcdfFile dataFile;
	
	protected long levelSize = 0;
	protected long latSize = 0;
	protected long lonSize = 0;
	protected long timeSize = 0;
	protected long airSize = 0;
	
    // Get the lat/lon data from the file.
    ArrayFloat.D1 levelArray = null;
    ArrayFloat.D1 latArray = null;
    ArrayFloat.D1 lonArray = null;
    ArrayDouble.D1 timeArray = null;
    ArrayShort.D4 airArray = null;

	ClimateData(String filename){
		this.filename = filename;
	}
	
	public void getClimateData(){
		try {
			dataFile = NetcdfFile.open(filename, null);
		
			// Get the latitude and longitude Variables.
			Variable levelVar = dataFile.findVariable("level");
			Variable latVar = dataFile.findVariable("lat");
			Variable lonVar = dataFile.findVariable("lon");
			Variable timeVar = dataFile.findVariable("time");
			Variable airVar = dataFile.findVariable("air");
	    
	    	levelArray = (ArrayFloat.D1) levelVar.read();
			latArray = (ArrayFloat.D1) latVar.read();
			lonArray = (ArrayFloat.D1) lonVar.read();
			timeArray = (ArrayDouble.D1) timeVar.read();
		    airArray = (ArrayShort.D4) airVar.read();
			
		    // Check the coordinate variable data.
		    levelSize = levelArray.getSize();
		    System.out.println("Level : " + levelSize);
		     
		    latSize = latArray.getSize();
		    System.out.println("Latitude : " + latSize);
		     
		    lonSize = lonArray.getSize();
		    System.out.println("Longitude : " + lonSize);
		     
		    timeSize = timeArray.getSize();
		    System.out.println("Time : " + timeSize);
		     
		    airSize = airArray.getSize();
		     
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public long getLevelSize(){
		return levelSize;
	}
	
	public long getLatSize(){
		return latSize;
	}
	
	public long getLonSize(){
		return lonSize;
	}
	
	public long getTimeSize(){
		return timeSize;
	}
	
	public long getAirSize(){
		return airSize;
	}
	
	public ArrayFloat.D1 getLevelArray(){
		return levelArray;
	}
	
	public ArrayFloat.D1 getLatArray(){
		return latArray;
	}
	
	public ArrayFloat.D1 getLonArray(){
		return lonArray;
	}
	
	public ArrayDouble.D1 getTimeArray(){
		return timeArray;
	}
	
	public ArrayShort.D4 getAirArray(){
		return airArray;
	}
}
