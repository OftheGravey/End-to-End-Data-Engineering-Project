package com.extractor.flink.functions;

import java.math.BigInteger;
import java.util.Base64;

public class CommonFunctions {
	public static double base64ToScaledDouble(String base64EncodedValue, int scale) {
		if (base64EncodedValue == null || base64EncodedValue.isEmpty()) {
			throw new IllegalArgumentException("Input string cannot be null or empty.");
		}
		byte[] decodedBytes;
		try {
			decodedBytes = Base64.getDecoder().decode(base64EncodedValue);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid Base64 string.", e);
		}
		BigInteger bigInteger = new BigInteger(1, decodedBytes);
		return bigInteger.doubleValue() / Math.pow(10, scale);
	}
}
