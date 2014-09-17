package org.apcc21.HBaseREST.utils;

import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.binary.Base64;

public class Decoder64 {
	public Decoder64(){
		
	}
	
	public static String decode64(String str){
		byte[] decoded = Base64.decodeBase64(str);
		String result = "";
		try {
			if (str.isEmpty())
				System.err.println("ERROR : Decoded String is empty!");
			else 
				result = new String(decoded, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
}