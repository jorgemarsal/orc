package org.apache.hadoop.hive.ql.io.orc;

/**
 * Reader which unpacks the bit packed values.
 */
public class BitPackReader {
	// current bit position
	static int current = 0;

	// bits left in a byte
	static int bitsLeft = 8;
	private static int numRead = 0;

	// bit packed array
	private static byte[] packed;

	/**
	 * Reads next byte (as integer) from bit packed array
	 */
	private static void readByte() {
		current = 0xff & packed[numRead++];
		bitsLeft = 8;
	}

	/**
	 * Unpack the bit packed input array till input array's length.
	 * @param inp
	 *          - bit packed array
	 * @param numBits
	 *          - bit width
	 * @return unpacked values. null is returned if input array is null or empty.
	 */
	public static long[] unpack(byte[] inp, int numBits) {
		if (inp == null || inp.length == 0) {
			return null;
		}

		return unpack(inp, inp.length, numBits);
	}

	/**
	 * Unpack the bit packed input array till the specified length.
	 * @param inp
	 *          - bit packed array
	 * @param n
	 *          - number of elements in the input array to unpack
	 * @param numBits
	 *          - bit width
	 * @return unpacked values. null for incorrect arguments
	 */
	public static long[] unpack(byte[] inp, int n, int numBits) {
		if (inp == null || inp.length < 1 || n < 1 || numBits < 1) {
			return null;
		}

		numRead = 0;
		packed = inp;

		// output unpacked array
		long[] unpacked = new long[n];
		if (inp.length > 0) {
			readByte();
		}

		for (int i = 0; i < n; i++) {
			long result = 0;
			int bitsLeftToRead = numBits;
			while (bitsLeftToRead > bitsLeft) {
				result <<= bitsLeft;
				result |= current & ((1 << bitsLeft) - 1);
				bitsLeftToRead -= bitsLeft;
				readByte();
			}

			// handle the left over bits
			if (bitsLeftToRead > 0) {
				result <<= bitsLeftToRead;
				bitsLeft -= bitsLeftToRead;
				result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
			}

			unpacked[i] = result;
		}
		return unpacked;
	}
}
