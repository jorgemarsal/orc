/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.Arrays;

class NewRunLengthIntegerWriter {

	public enum EncodingType {
		SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
	}

	static final int MAX_SCOPE = 512;
	static final int MIN_REPEAT = 3;
	private static final int MAX_SHORT_REPEAT_LENGTH = 10;
	private static final int MAX_SHORT_REPEAT_SIZE = 8;
	private long prevDelta = 0;
	private int fixedRunLength = 0;
	private int variableRunLength = 0;
	private long[] literals = new long[MAX_SCOPE];
	private final PositionedOutputStream output;
	private final boolean signed;
	private EncodingType encoding = null;
	private int numLiterals = 0;
	private long min = 0;
	private long max = 0;
	private long[] zigzagLiterals = null;
	private long[] zzBaseReduced = null;
	private long[] zzAdjDeltas = null;
	private long fixedDelta = 0;
	private int zzBits90p = 0;
	private int zzBits100p = 0;
	private int zzBRBits95p = 0;
	private int zzBRBits100p = 0;
	private int bitsDeltaMax = 0;
	private int patchWidth = 0;
	private int patchGapWidth = 0;
	private int patchLength = 0;
	private long[] gapVsPatchList = null;
	private long zzMin = 0;
	private boolean isFixedDelta = false;

	NewRunLengthIntegerWriter(PositionedOutputStream output, boolean signed) {
		this.output = output;
		this.signed = signed;
	}

	private void writeValues() throws IOException {
		if (numLiterals != 0) {

			if (encoding.equals(EncodingType.SHORT_REPEAT)) {
				writeShortRepeatValues();
			} else if (encoding.equals(EncodingType.DIRECT)) {
				writeDirectValues();
			} else if (encoding.equals(EncodingType.PATCHED_BASE)) {
				writePatchedBaseValues();
			} else {
				writeDeltaValues();
			}

			// clear all the variables
			clear();
		}
	}

	private void writeDeltaValues() throws IOException {
		// write encoding type in top 2 bits
		int enc = encoding.ordinal() << 6;

		int len = 0;
		int fb = bitsDeltaMax;
		int cfb = 0;
		int fbo = 0;

		if (isFixedDelta) {
			// if the fixed delta is 0 then the sequence is counted as fixed
			// run length else as variable run length
			if (fixedRunLength > MIN_REPEAT) {
				// ex. sequence: 2 2 2 2 2 2 2 2
				len = fixedRunLength - 1;
				fixedRunLength = 0;
			} else {
				// ex. sequence: 4 6 8 10 12 14 16
				len = variableRunLength - 1;
				variableRunLength = 0;
			}
		} else {
			cfb = FixedBitSizes.getClosestFixedBits(fb);
			// fixed width 0 is used for fixed delta runs. sequence that incur
			// only 1 bit to encode will have an additional bit
			if (cfb == 1) {
				cfb = 2;
			}
			fbo = FixedBitSizes.fixedBitsToOrdinal(cfb) << 1;
			len = variableRunLength - 1;
			variableRunLength = 0;
		}

		// extract the 9th bit of run length
		int tailBits = (len & 0x100) >>> 8;

		// create first byte of the header
		int headerFirstByte = enc | fbo | tailBits;

		// second byte of the header stores the remaining 8 bits of runlength
		int headerSecondByte = len & 0xff;

		// write header
		output.write(headerFirstByte);
		output.write(headerSecondByte);

		// store the first value from zigzag literal array
		SerializationUtils.writeVslong(output, literals[0]);

		// if delta is fixed then we don't need to store delta blob else store
		// delta blob using fixed bit packing
		if (isFixedDelta) {
			// store the fixed delta using zigzag encoding
			SerializationUtils.writeVslong(output, fixedDelta);
		} else {
			// bit pack the delta blob
			byte[] packed = BitPackWriter.pack(zzAdjDeltas, zzAdjDeltas.length, cfb);
			for (int i = 0; i < packed.length; i++) {
				output.write(packed[i]);
			}
		}
	}

	private void writePatchedBaseValues() throws IOException {
		// write encoding type in top 2 bits
		int enc = encoding.ordinal() << 6;

		// write the number of fixed bits required in next 5 bits
		int fb = zzBRBits95p;
		int fbc = FixedBitSizes.getClosestFixedBits(fb);
		int fbo = FixedBitSizes.fixedBitsToOrdinal(fbc) << 1;

		// adjust variable run length, they are one off
		variableRunLength -= 1;

		// extract the 9th bit of run length
		int tailBits = (variableRunLength & 0x100) >>> 8;

		// create first byte of the header
		int headerFirstByte = enc | fbo | tailBits;

		// second byte of the header stores the remaining 8 bits of runlength
		int headerSecondByte = variableRunLength & 0xff;

		// find the number of bytes required for base and shift it by 5 bits
		// to accommodate patch width
		int baseWidth = Utils.findNumBits(zzMin);
		baseWidth = FixedBitSizes.getClosestFixedBits(baseWidth);
		int baseBytes = baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
		int bb = (baseBytes - 1) << 5;

		// third byte contains 3 bits for number of bytes occupied by base
		// and 5 bits for patchWidth
		int headerThirdByte = bb | FixedBitSizes.fixedBitsToOrdinal(patchWidth);

		// fourth byte contains 3 bits for page gap width and 5 bits for
		// patch length
		int headerFourthByte = (patchGapWidth - 1) << 5 | patchLength;

		// write header
		output.write(headerFirstByte);
		output.write(headerSecondByte);
		output.write(headerThirdByte);
		output.write(headerFourthByte);

		// write the base value using fixed bytes in big endian order
		for (int i = baseBytes - 1; i >= 0; i--) {
			byte b = (byte) ((zzMin >>> (i * 8)) & 0xff);
			output.write(b);
		}

		// bit packing the delta values and write each bytes
		int closestFixedBits = FixedBitSizes.getClosestFixedBits(zzBRBits95p);
		byte[] packed = BitPackWriter.pack(zzBaseReduced, numLiterals,
		    closestFixedBits);
		for (int i = 0; i < packed.length; i++) {
			output.write(packed[i]);
		}

		// write the patch blob
		closestFixedBits = FixedBitSizes.getClosestFixedBits(patchGapWidth
		    + patchWidth);
		packed = BitPackWriter.pack(gapVsPatchList, patchLength, closestFixedBits);
		for (int i = 0; i < packed.length; i++) {
			output.write(packed[i]);
		}

		// reset run length
		variableRunLength = 0;
	}

	private void writeDirectValues() throws IOException {
		// write encoding type in top 2 bits
		int enc = encoding.ordinal() << 6;

		// write the number of fixed bits required in next 5 bits
		int cfb = zzBits100p;
		int fbo = FixedBitSizes.fixedBitsToOrdinal(cfb) << 1;

		// adjust variable run length
		variableRunLength -= 1;

		// extract the 9th bit of run length
		int tailBits = (variableRunLength & 0x100) >>> 8;

		// create first byte of the header
		int headerFirstByte = enc | fbo | tailBits;

		// second byte of the header stores the remaining 8 bits of
		// runlength
		int headerSecondByte = variableRunLength & 0xff;

		// write header
		output.write(headerFirstByte);
		output.write(headerSecondByte);

		// bit packing the delta values and write each bytes
		byte[] packed = BitPackWriter.pack(zigzagLiterals, zigzagLiterals.length,
		    cfb);
		for (int i = 0; i < packed.length; i++) {
			output.write(packed[i]);
		}

		// reset run length
		variableRunLength = 0;
	}

	private void writeShortRepeatValues() throws IOException {
		// System.out.println("SHORT_REPEAT: " + numLiterals + " Literals: "
		// + Longs.asList(Arrays.copyOf(literals, numLiterals)));

		// get the value that is repeating, compute the bits and bytes required
		long repeatVal = Utils.zigzagEncode(literals[0]);
		int numBitsRepeatVal = Utils.findNumBits(repeatVal);
		numBitsRepeatVal = FixedBitSizes.getClosestFixedBits(numBitsRepeatVal);
		int numBytesRepeatVal = numBitsRepeatVal % 8 == 0 ? numBitsRepeatVal >>> 3
		    : (numBitsRepeatVal >>> 3) + 1;

		// if the runs are long or too short and if the delta is non zero, then
		// choose a different algorithm
		if (fixedRunLength >= MIN_REPEAT
		    && fixedRunLength <= MAX_SHORT_REPEAT_LENGTH
		    && numBytesRepeatVal <= MAX_SHORT_REPEAT_SIZE && prevDelta == 0) {
			// write encoding type in top 2 bits
			int header = encoding.ordinal() << 6;

			// write the number of bytes required for the value
			header |= ((numBytesRepeatVal - 1) << 3);

			// write the run length
			fixedRunLength -= MIN_REPEAT;
			header |= fixedRunLength;

			// write the header
			output.write(header);

			// write the payload (i.e. the repeat value) in big endian
			for (int i = numBytesRepeatVal - 1; i >= 0; i--) {
				int b = (int) ((repeatVal >>> (i * 8)) & 0xff);
				output.write(b);
			}

			fixedRunLength = 0;
		} else {
			determineEncoding();
			writeValues();
		}
	}

	private void determineEncoding() {
		// used for direct encoding
		zigzagLiterals = new long[numLiterals];

		// used for patched base encoding
		zzBaseReduced = new long[numLiterals];

		// used for delta encoding
		zzAdjDeltas = new long[numLiterals - 1];

		int idx = 0;

		// for identifying monotonic sequences
		boolean isIncreasing = false;
		int increasingCount = 1;
		boolean isDecreasing = false;
		int decreasingCount = 1;

		// for identifying type of delta encoding
		long currMin = literals[0];
		long currMax = literals[0];
		isFixedDelta = true;
		long currDelta = 0;

		zzMin = Utils.zigzagEncode(literals[0]);
		long deltaMax = 0;

		// populate all variables to identify the encoding type
		if (numLiterals >= 1) {
			currDelta = literals[1] - literals[0];
			for (int i = 0; i < numLiterals; i++) {
				if (i > 0 && literals[i] >= currMax) {
					currMax = literals[i];
					increasingCount++;
				}

				if (i > 0 && literals[i] <= currMin) {
					currMin = literals[i];
					decreasingCount++;
				}

				// if delta doesn't changes then mark it as fixed delta
				if (i > 0 && isFixedDelta) {
					if (literals[i] - literals[i - 1] != currDelta) {
						isFixedDelta = false;
					}

					fixedDelta = currDelta;
				}

				// store the minimum value among zigzag encoded values. The min
				// value (base) will be removed in patched base encoding
				long zzEncVal = Utils.zigzagEncode(literals[i]);
				if (zzEncVal < zzMin) {
					zzMin = zzEncVal;
				}
				zigzagLiterals[idx] = zzEncVal;
				idx++;

				// max delta value is required for computing the fixed bits
				// required for delta blob in delta encoding
				if (i > 0) {
					zzAdjDeltas[i - 1] = Utils
					    .zigzagEncode(literals[i] - literals[i - 1]);
					if (zzAdjDeltas[i - 1] > deltaMax) {
						deltaMax = zzAdjDeltas[i - 1];
					}
				}
			}

			// stores the number of bits required for packing delta blob in
			// delta encoding
			bitsDeltaMax = Utils.findNumBits(deltaMax);

			// if decreasing count equals total number of literals then the
			// sequence is monotonically decreasing
			if (increasingCount == 1 && decreasingCount == numLiterals) {
				isDecreasing = true;
			}

			// if increasing count equals total number of literals then the
			// sequence is monotonically increasing
			if (decreasingCount == 1 && increasingCount == numLiterals) {
				isIncreasing = true;
			}
		}

		// if the sequence is both increasing and decreasing then it is not
		// monotonic
		if (isDecreasing && isIncreasing) {
			isDecreasing = false;
			isIncreasing = false;
		}

		// percentile values are computed for the zigzag encoded values. if the
		// number of bit requirement between 90th and 100th percentile varies
		// beyond a threshold then we need to patch the values. if the variation
		// is not significant then we can use direct or delta encoding

		// percentile is called multiple times and so we will sort the array
		// once and reuse it
		long[] sortedZigzag = Arrays.copyOf(zigzagLiterals, zigzagLiterals.length);
		Arrays.sort(sortedZigzag);
		double p = 0.9;
		long p90Val = (long) Utils.percentile(sortedZigzag, p, true);
		zzBits90p = Utils.findNumBits(p90Val);
		zzBits90p = FixedBitSizes.getClosestFixedBits(zzBits90p);

		p = 1.0;
		long p100Val = (long) Utils.percentile(sortedZigzag, p, true);
		zzBits100p = Utils.findNumBits(p100Val);
		zzBits100p = FixedBitSizes.getClosestFixedBits(zzBits100p);

		int diffBitsLH = zzBits100p - zzBits90p;

		// if the difference between 90th percentile and 100th percentile fixed
		// bits is > 1 then we need patch the values
		if (isIncreasing == false && isDecreasing == false && diffBitsLH > 1
		    && isFixedDelta == false) {
			// patching is done only on base reduces values.
			// remove base from the zigzag encoded values
			for (int i = 0; i < zigzagLiterals.length; i++) {
				zzBaseReduced[i] = zigzagLiterals[i] - zzMin;
			}

			// percentile is com
			long[] sortedZZBaseReduced = Arrays.copyOf(zzBaseReduced,
			    zzBaseReduced.length);
			Arrays.sort(sortedZZBaseReduced);

			// 95th percentile width is used to determine max allowed value
			// after which patching will be done
			p = 0.95;
			long p95Val = (long) Utils.percentile(sortedZZBaseReduced, p, true);
			zzBRBits95p = Utils.findNumBits(p95Val);
			zzBRBits95p = FixedBitSizes.getClosestFixedBits(zzBRBits95p);

			// 100th percentile is used to compute the max patch width
			p = 1.0;
			p100Val = (long) Utils.percentile(sortedZZBaseReduced, p, true);
			zzBRBits100p = Utils.findNumBits(p100Val);
			zzBRBits100p = FixedBitSizes.getClosestFixedBits(zzBRBits100p);

			// after base reducing the values, if the difference in bits between
			// 95th percentile and 100th percentile value is zero then there
			// is no point in patching the values, in which case we will
			// fallback to DIRECT encoding. The reason for this is max value
			// (beyond which patching is done) is based on 95th percentile
			// base reduced value.
			if ((zzBRBits100p - zzBRBits95p) != 0) {
				encoding = EncodingType.PATCHED_BASE;
				preparePatchedBlob();
				// System.out.println("PATCHED_BASE: " + numLiterals
				// + " Literals: " + Longs.asList(zzBaseReduced));
				return;
			} else {
				encoding = EncodingType.DIRECT;
				return;
			}
		}

		// if difference in bits between 95th percentile and 100th percentile is
		// 0, then patch length will become 0. Hence we will fallback to direct
		if (isIncreasing == false && isDecreasing == false && diffBitsLH <= 1
		    && isFixedDelta == false) {
			encoding = EncodingType.DIRECT;
			// System.out.println("DIRECT: " + numLiterals + " Literals: "
			// + Longs.asList(zigzagLiterals));
			return;
		}

		if (isIncreasing == false && isDecreasing == false && diffBitsLH <= 1
		    && isFixedDelta == true) {
			encoding = EncodingType.DELTA;
			// System.out.println("DELTA (fixed delta): " + numLiterals
			// + " Literals: " + Longs.asList(zigzagLiterals));
			return;
		}

		if (isIncreasing || isDecreasing) {
			encoding = EncodingType.DELTA;
			// System.out.println("DELTA (variable delta monotonic): "
			// + numLiterals + " Literals: "
			// + Longs.asList(zigzagLiterals));
			return;
		}

		if (encoding == null) {
			throw new RuntimeException("Integer encoding cannot be determined.");
		}
	}

	private void preparePatchedBlob() {
		// mask will be max value beyond which patch will be generated
		int mask = (1 << zzBRBits95p) - 1;

		// since we are considering only 95 percentile, the size of gap and
		// patch array can contain only be 5% values
		patchLength = (int) Math.ceil((zzBaseReduced.length * 0.05));
		int[] gapList = new int[patchLength];
		long[] patchList = new long[patchLength];

		// #bit for patch
		patchWidth = zzBRBits100p - zzBRBits95p;
		patchWidth = FixedBitSizes.getClosestFixedBits(patchWidth);

		int gapIdx = 0;
		int patchIdx = 0;
		int prev = 0;
		int gap = 0;
		int maxGap = 0;

		for (int i = 0; i < zzBaseReduced.length; i++) {
			// if value is above mask then create the patch and record the gap
			if (zzBaseReduced[i] > mask) {
				gap = i - prev;
				if (gap > maxGap) {
					maxGap = gap;
				}

				// gaps are relative, so store the previous patched value
				prev = i;
				gapList[gapIdx++] = gap;

				// extract the most significant bits that are over mask
				long patch = zzBaseReduced[i] >>> zzBRBits95p;
				patchList[patchIdx++] = patch;

				// strip off the MSB to enable safe bit packing
				zzBaseReduced[i] &= mask;
			}
		}

		// adjust the patch length to number of entries in gap list
		patchLength = gapIdx;

		if (patchLength == 0) {
			System.out.println(zzBRBits95p + ":" + mask + ":" + zzBRBits100p);
		}
		// if the element to be patched is the first and only element then
		// max gap will be 0, but to store the gap as 0 we need atleast 1 bit
		if (maxGap == 0 && patchLength != 0) {
			patchGapWidth = 1;
		} else {
			patchGapWidth = Utils.findNumBits(maxGap);
		}

		// special case: if the patch gap width is greater than 256, then
		// we need 9 bits to encode the gap width. But we only have 3 bits in
		// header to record the gap width. To deal with this case, we will save
		// two entries in final patch list with following entries
		// 256 gap width => 0 for patch value
		// actual gap - 256 => actual patch value
		if (patchGapWidth > 8) {
			patchGapWidth = 8;
			// for gap = 511, we need two additional entries in patch list
			if (maxGap == 511) {
				patchLength += 2;
			} else {
				patchLength += 1;
			}
		}

		// create gap vs patch list
		gapIdx = 0;
		patchIdx = 0;
		gapVsPatchList = new long[patchLength];
		for (int i = 0; i < patchLength; i++) {
			long g = gapList[gapIdx++];
			long p = patchList[patchIdx++];
			while (g > 255) {
				gapVsPatchList[i++] = (255 << patchWidth) | 0;
				g -= 255;
			}

			// store patch value in LSBs and gap in MSBs
			gapVsPatchList[i] = (g << patchWidth) | p;
		}
	}

	/**
	 * clears all the variables
	 */
	private void clear() {
		numLiterals = 0;
		min = 0;
		max = 0;
		encoding = null;
		prevDelta = 0;
		zigzagLiterals = null;
		zzBaseReduced = null;
		zzAdjDeltas = null;
		fixedDelta = 0;
		zzBits90p = 0;
		zzBits100p = 0;
		zzBRBits95p = 0;
		zzBRBits100p = 0;
		bitsDeltaMax = 0;
		patchGapWidth = 0;
		patchLength = 0;
		patchWidth = 0;
		gapVsPatchList = null;
		zzMin = 0;
		isFixedDelta = false;
	}

	void flush() throws IOException {
		// if only one element is left in buffer then use short repeat encoding
		if (numLiterals == 1) {
			encoding = EncodingType.SHORT_REPEAT;
			fixedRunLength = 1;
			writeValues();
		}

		// if variable runs are not 0 then determine the delta encoding to use
		// and flush out the buffer
		if (variableRunLength != 0 && numLiterals != 0) {
			determineEncoding();
			writeValues();
		}

		// if fixed runs are not 0 then flush out the buffer
		if (fixedRunLength != 0 && numLiterals != 0) {
			// encoding = EncodingType.SHORT_REPEAT;
			if (fixedRunLength < MIN_REPEAT) {
				variableRunLength = fixedRunLength;
				fixedRunLength = 0;
				determineEncoding();
				writeValues();
			} else {
				encoding = EncodingType.SHORT_REPEAT;
				writeValues();
			}
		}

		output.flush();
	}

	void write(long val) throws IOException {
		if (numLiterals == 0) {
			initializeLiterals(val);
		} else {
			// update min and max values
			if (val <= min) {
				min = val;
			}

			if (val >= max) {
				max = val;
			}

			if (numLiterals == 1) {
				prevDelta = val - literals[0];
				literals[numLiterals++] = val;
				// if both values are same count as fixed run else variable run
				if (val == literals[0]) {
					fixedRunLength = 2;
					variableRunLength = 0;
				} else {
					fixedRunLength = 0;
					variableRunLength = 2;
				}
			} else {
				long currentDelta = val - literals[numLiterals - 1];
				if (prevDelta == 0 && currentDelta == 0) {
					// fixed delta run

					literals[numLiterals++] = val;

					// if variable run is non-zero then we are seeing repeating
					// values at the end of variable run in which case keep
					// updating variable and fixed runs
					if (variableRunLength > 0) {
						fixedRunLength = 2;
					}
					fixedRunLength += 1;

					// if fixed run met the minimum condition and if variable
					// run is non-zero then flush the variable run and shift the
					// tail fixed runs to start of the buffer
					if (fixedRunLength >= MIN_REPEAT && variableRunLength > 0) {
						numLiterals -= MIN_REPEAT;
						variableRunLength -= MIN_REPEAT - 1;
						// copy the tail fixed runs
						long[] tailVals = Arrays.copyOfRange(literals, numLiterals,
						    numLiterals + MIN_REPEAT);

						// determine variable encoding and flush values
						determineEncoding();
						writeValues();

						// shift tail fixed runs to beginning of the buffer
						for (long l : tailVals) {
							literals[numLiterals++] = l;
						}
					}

					// if fixed runs reached max repeat length then write values
					if (fixedRunLength == MAX_SCOPE) {
						determineEncoding();
						writeValues();
					}
				} else {
					// variable delta run

					// if fixed run length is non-zero and if satisfies the
					// short repeat algorithm conditions then write the values
					// as short repeats else determine the appropriate algorithm
					// to persist the values
					if (fixedRunLength >= MIN_REPEAT) {
						if (fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
							encoding = EncodingType.SHORT_REPEAT;
							writeValues();
						} else {
							determineEncoding();
							writeValues();
						}
					}

					// if fixed run length is <MIN_REPEAT and current value is
					// different from previous then treat it as variable run
					if (fixedRunLength > 0 && fixedRunLength < MIN_REPEAT) {
						if (val != literals[numLiterals - 1]) {
							variableRunLength = fixedRunLength;
							fixedRunLength = 0;
						}
					}

					// after writing values re-initialize the variables
					if (numLiterals == 0) {
						initializeLiterals(val);
					} else {
						// keep updating variable run lengths
						prevDelta = val - literals[numLiterals - 1];
						literals[numLiterals++] = val;
						variableRunLength += 1;

						// if variable run length reach the max scope, write it
						if (variableRunLength == MAX_SCOPE) {
							determineEncoding();
							writeValues();
						}
					}

				}
			}
		}
	}

	private void initializeLiterals(long val) {
		literals[numLiterals++] = val;
		min = val;
		max = val;
		fixedRunLength = 1;
		variableRunLength = 1;
	}

	void getPosition(PositionRecorder recorder) throws IOException {
		output.getPosition(recorder);
		recorder.addPosition(numLiterals);
	}
}
