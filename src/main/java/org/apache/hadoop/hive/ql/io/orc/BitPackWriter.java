package org.apache.hadoop.hive.ql.io.orc;

/**
 * Writer which bit packs integer values.
 */

public class BitPackWriter {
	static int bytesInLong = Long.SIZE / 8;
	static byte[] packed;
	static int numPacked = 0;
	static byte current = 0;
	static int bitsLeft = 8;

	/**
	 * If there are any left over bits then flush the final byte
	 */
	private static void flush() {
		if (bitsLeft != 8) {
			writeByte();
		}
	}

	private static void writeByte() {
		packed[numPacked++] = current;
		current = 0;
		bitsLeft = 8;
	}

	/**
	 * Bit packs the input array for array length of values
	 * @param inp
	 *          - input array
	 * @param n
	 *          - number of elements in the array to bit pack
	 * @param numBits
	 *          - bit width
	 * @return bit packed byte array, null returned for any illegal argument
	 */
	public static byte[] pack(long[] inp, int numBits) {
		if (inp == null || inp.length == 0 || numBits < 1) {
			return null;
		}

		return pack(inp, inp.length, numBits);
	}

	/**
	 * Bit packs the input array for specified length
	 * @param inp
	 *          - input array
	 * @param n
	 *          - number of elements in the array to bit pack
	 * @param numBits
	 *          - bit width
	 * @return bit packed byte array, null returned for any illegal argument
	 */
	public static byte[] pack(long[] inp, int n, int numBits) {
		if (inp == null || n < 1 || inp.length == 0 || numBits < 1) {
			return null;
		}

		numPacked = 0;
		int totalBytes = getTotalBytesRequired(n, numBits);
		packed = new byte[totalBytes];

		for (int i = 0; i < n; i++) {
			long value = inp[i];
			int bitsToWrite = numBits;
			while (bitsToWrite > bitsLeft) {
				// add the bits to the bottom of the current word
				current |= value >>> (bitsToWrite - bitsLeft);
				// subtract out the bits we just added
				bitsToWrite -= bitsLeft;
				// zero out the bits above bitsToWrite
				value &= (1L << bitsToWrite) - 1;
				writeByte();
			}
			bitsLeft -= bitsToWrite;
			current |= value << bitsLeft;
			if (bitsLeft == 0) {
				writeByte();
			}
		}

		// flush the left over bytes
		flush();
		return packed;
	}

	/**
	 * Calculate the number of bytes required
	 * @param n
	 *          - number of values
	 * @param numBits
	 *          - bit width
	 * @return number of bytes required
	 */
	public static int getTotalBytesRequired(int n, int numBits) {
		return (int) Math.ceil((double) (n * numBits) / 8.0);
	}

}
