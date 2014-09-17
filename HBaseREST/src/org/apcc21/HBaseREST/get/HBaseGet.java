package org.apcc21.HBaseREST.get;

import java.io.IOException;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.apcc21.HBaseREST.get.ClimateDataClass.Cell;
import org.apcc21.HBaseREST.get.ClimateDataClass.Row;
import org.apcc21.HBaseREST.utils.Decoder64;

import com.google.gson.Gson;

public class HBaseGet {
	public String url = "";
	public String row = "";
	public HBaseHttpGet httpGet;
	
	HBaseGet(String url, String row){
		this.url = url;
		this.row = row;
		httpGet = new HBaseHttpGet(url + row);
	}
	
	public String get(){
		String result = "";
		
		try {
			result = httpGet.get();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static void main(String[] args) {
		String url = "http://210.98.49.153:8080/climate/";
		String row = "row36000";
		
		HBaseGet hbaseGet = new HBaseGet(url, row);
		String json = hbaseGet.get();
		System.out.println("Json : " + json);
		ClimateDataClass climateDataClass = new Gson().fromJson(json, ClimateDataClass.class);
		List<Row> data = climateDataClass.getRow();
		
		System.out.println("output : " + Decoder64.decode64(data.get(0).getKey()));
	}
}