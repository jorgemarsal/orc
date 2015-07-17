package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Random;

import org.junit.Test;

import com.google.common.primitives.Longs;

public class TestBitPack {

	private static final int SIZE = 100;
	private static Random rand = new Random(100);

	private void runTest(int numBits) {
		long[] inp = new long[SIZE];
		for (int i = 0; i < SIZE; i++) {
			long val = 0;
			if (numBits <= 32) {
				val = rand.nextInt((int) Math.pow(2, numBits - 1));
			} else {
				val = Utils.nextLong(rand, (long) Math.pow(2, numBits - 2));
			}
			if (val % 2 == 0) {
				val = -val;
			}
			inp[i] = val;
		}
		long[] deltaEncoded = Utils.deltaEncode(inp);
		long minInput = Collections.min(Longs.asList(deltaEncoded));
		long maxInput = Collections.max(Longs.asList(deltaEncoded));
		long rangeInput = maxInput - minInput;
		int fixedWidth = Utils.findNumBits(rangeInput);
		fixedWidth = FixedBitSizes.getClosestFixedBits(fixedWidth);
		byte[] packed = BitPackWriter.pack(deltaEncoded, deltaEncoded.length,
		    fixedWidth);
		long[] deltaDec = BitPackReader.unpack(packed, deltaEncoded.length,
		    fixedWidth);
		long[] out = new long[deltaDec.length];
		for (int i = 0; i < deltaDec.length; i++) {
			out[i] = Utils.zigzagDecode(deltaDec[i]);
		}
		assertEquals(numBits, fixedWidth);
		assertArrayEquals(inp, out);
	}

	@Test
	public void test01BitPacking1Bit() {
		long[] inp = new long[SIZE];
		for (int i = 0; i < SIZE; i++) {
			inp[i] = -1 * rand.nextInt(2);
		}
		long[] deltaEncoded = Utils.deltaEncode(inp);
		long minInput = Collections.min(Longs.asList(deltaEncoded));
		long maxInput = Collections.max(Longs.asList(deltaEncoded));
		long rangeInput = maxInput - minInput;
		int fixedWidth = Utils.findNumBits(rangeInput);
		fixedWidth = FixedBitSizes.getClosestFixedBits(fixedWidth);
		byte[] packed = BitPackWriter.pack(deltaEncoded, deltaEncoded.length,
		    fixedWidth);
		long[] deltaDec = BitPackReader.unpack(packed, SIZE, fixedWidth);
		long[] out = new long[deltaDec.length];
		for (int i = 0; i < deltaDec.length; i++) {
			out[i] = Utils.zigzagDecode(deltaDec[i]);
		}
		assertEquals(1, fixedWidth);
		assertArrayEquals(inp, out);
	}

	@Test
	public void test02BitPacking2Bit() {
		runTest(2);
	}

	@Test
	public void test03BitPacking3Bit() {
		runTest(3);
	}

	@Test
	public void test04BitPacking4Bit() {
		runTest(4);
	}

	@Test
	public void test05BitPacking5Bit() {
		runTest(5);
	}

	@Test
	public void test06BitPacking6Bit() {
		runTest(6);
	}

	@Test
	public void test07BitPacking7Bit() {
		runTest(7);
	}

	@Test
	public void test08BitPacking8Bit() {
		runTest(8);
	}

	@Test
	public void test09BitPacking9Bit() {
		runTest(9);
	}

	@Test
	public void test10BitPacking10Bit() {
		runTest(10);
	}

	@Test
	public void test11BitPacking11Bit() {
		runTest(11);
	}

	@Test
	public void test12BitPacking12Bit() {
		runTest(12);
	}

	@Test
	public void test13BitPacking13Bit() {
		runTest(13);
	}

	@Test
	public void test14BitPacking14Bit() {
		runTest(14);
	}

	@Test
	public void test15BitPacking15Bit() {
		runTest(15);
	}

	@Test
	public void test16BitPacking16Bit() {
		runTest(16);
	}

	@Test
	public void test17BitPacking17Bit() {
		runTest(17);
	}

	@Test
	public void test18BitPacking18Bit() {
		runTest(18);
	}

	@Test
	public void test19BitPacking19Bit() {
		runTest(19);
	}

	@Test
	public void test20BitPacking20Bit() {
		runTest(20);
	}

	@Test
	public void test21BitPacking21Bit() {
		runTest(21);
	}

	@Test
	public void test22BitPacking22Bit() {
		runTest(22);
	}

	@Test
	public void test23BitPacking23Bit() {
		runTest(23);
	}

	@Test
	public void test24BitPacking24Bit() {
		runTest(24);
	}

	@Test
	public void test26BitPacking26Bit() {
		runTest(26);
	}

	@Test
	public void test28BitPacking28Bit() {
		runTest(28);
	}

	@Test
	public void test30BitPacking30Bit() {
		runTest(30);
	}

	@Test
	public void test32BitPacking32Bit() {
		runTest(32);
	}

	@Test
	public void test40BitPacking40Bit() {
		runTest(40);
	}

	@Test
	public void test48BitPacking48Bit() {
		runTest(48);
	}

	@Test
	public void test56BitPacking56Bit() {
		runTest(56);
	}

	@Test
	public void test64BitPacking64Bit() {
		runTest(64);
	}
}
