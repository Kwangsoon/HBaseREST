package org.apcc21.HBaseREST.utils;

import org.apache.commons.codec.binary.Base64;

public class Encoder64 {
	
	public static String encode64(String str){
		Base64 base64 = new Base64();
		byte[] encodedBytes = base64.encodeBase64((str).getBytes());
		String row64 = new String (encodedBytes);
		return row64;
	}
}
