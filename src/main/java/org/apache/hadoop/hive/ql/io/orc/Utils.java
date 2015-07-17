package org.apache.hadoop.hive.ql.io.orc;

import java.util.Arrays;
import java.util.Random;

public class Utils {

	/**
	 * Count the number of bits required to encode the given value
	 * @param value
	 * @return bits required to store value
	 */
	public static int findNumBits(long value) {
		int count = 0;
		while (value > 0) {
			count++;
			value = value >>> 1;
		}
		return count;
	}

	// this is used only for testing
	public static long[] deltaEncode(long[] inp) {
		long[] output = new long[inp.length];
		for (int i = 0; i < inp.length; i++) {
			output[i] = zigzagEncode(inp[i]);
		}
		return output;
	}

	/**
	 * zigzag encode the given value
	 * @param val
	 * @return zigzag encoded value
	 */
	public static long zigzagEncode(long val) {
		return (val << 1) ^ (val >> 63);
	}

	/**
	 * zigzag decode the given value
	 * @param val
	 * @return zizag decoded value
	 */
	public static long zigzagDecode(long val) {
		return (val >>> 1) ^ -(val & 1);
	}

	/**
	 * Next random long value
	 * @param rng
	 * @param n
	 * @return random long value
	 */
	public static long nextLong(Random rng, long n) {
		long bits, val;
		do {
			bits = (rng.nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits - val + (n - 1) < 0L);
		return val;
	}

	public static double percentile(long[] data, double p, boolean sorted) {
		if ((p > 1.0) || (p <= 0.0)) {
			throw new IllegalArgumentException("invalid percentile value: " + p);
		}

		long[] input = data;

		if (sorted == false) {
			input = Arrays.copyOf(data, data.length);
			Arrays.sort(input);
		}

		int n = input.length;
		int idx = (int) Math.floor((n + 1) * p);
		if (idx >= n) {
			return input[n - 1];
		}

		if (idx < 1) {
			return input[0];
		}
		long lower = input[idx - 1];
		long upper = input[idx];
		double diff = ((n + 1) * p) - idx;
		return lower + diff * (upper - lower);
	}

	public static long[] copyRangeAndZigzagEncode(long[] literals, int start,
	    int end) {
		if (end < start) {
			return null;
		}
		long[] out = new long[end - start];
		int idx = 0;
		for (int i = start; i < end; i++) {
			out[idx++] = zigzagEncode(literals[i]);
		}

		return out;
	}

	public static long bytesToLongBE(byte[] b) {
		long out = 0;

		int offset = b.length - 1;
		for (int i = 0; i < b.length; i++) {
			long val = 0xff & b[i];
			out |= (val << (offset * 8));
			offset -= 1;
		}

		return out;
	}
}
