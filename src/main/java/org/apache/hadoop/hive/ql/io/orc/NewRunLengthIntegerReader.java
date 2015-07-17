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

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.NewRunLengthIntegerWriter.EncodingType;

/**
 * A reader that reads a sequence of integers.
 */
class NewRunLengthIntegerReader {
	private final InStream input;
	private final boolean signed;
	private final long[] literals = new long[NewRunLengthIntegerWriter.MAX_SCOPE];
	private int numLiterals = 0;
	private int used = 0;

	NewRunLengthIntegerReader(InStream input, boolean signed) throws IOException {
		this.input = input;
		this.signed = signed;
	}

	private void readValues() throws IOException {
		// read the first 2 bits and determine the encoding type
		int firstByte = input.read();
		if (firstByte < 0) {
			throw new EOFException("Read past end of RLE integer from " + input);
		} else {
			int enc = (firstByte >>> 6) & 0x03;
			if (EncodingType.SHORT_REPEAT.ordinal() == enc) {
				readShortRepeatValues(firstByte);
			} else if (EncodingType.DIRECT.ordinal() == enc) {
				readDirectValues(firstByte);
			} else if (EncodingType.PATCHED_BASE.ordinal() == enc) {
				readPatchedBaseValues(firstByte);
			} else {
				readDeltaValues(firstByte);
			}
		}
	}

	private void readDeltaValues(int firstByte) throws IOException {

		// extract the number of fixed bits
		int fb = (firstByte >>> 1) & 0x1f;
		if (fb != 0) {
			fb = FixedBitSizes.ordinalToFixedBits(fb);
		}

		// extract the blob run length
		int len = (firstByte & 0x01) << 8;
		len |= input.read();

		// read the first value stored as vint
		long firstVal = SerializationUtils.readVslong(input);

		// store first value to result buffer
		long prevVal = firstVal;
		literals[numLiterals++] = firstVal;

		// if fixed bits is 0 then all values have fixed delta
		if (fb == 0) {
			// read the fixed delta value stored as vint
			long fd = SerializationUtils.readVslong(input);

			// add fixed deltas to adjacent values
			for (int i = 0; i < len; i++) {
				literals[numLiterals++] = literals[numLiterals - 2] + fd;
			}
		} else {
			// number of bytes of data blob
			int bytesToRead = BitPackWriter.getTotalBytesRequired(len, fb);

			// create buffer space to fill up the blob and unpack the bit packed
			// values
			byte[] buffer = new byte[bytesToRead];
			for (int i = 0; i < bytesToRead; i++) {
				buffer[i] = (byte) input.read();
			}

			// write the unpacked values, zigzag decode the delta values, add
			// it to previous value and store final value to result buffer
			long[] adjDeltas = BitPackReader.unpack(buffer, len, fb);
			for (int i = 0; i < adjDeltas.length; i++) {
				long zzDelta = Utils.zigzagDecode(adjDeltas[i]);
				literals[numLiterals++] = prevVal + zzDelta;
				prevVal = literals[numLiterals - 1];
			}
		}

	}

	private void readPatchedBaseValues(int firstByte) throws IOException {

		// extract the number of fixed bits
		int fbo = (firstByte >>> 1) & 0x1f;
		int fb = FixedBitSizes.ordinalToFixedBits(fbo);

		// extract the run length of data blob
		int len = (firstByte & 0x01) << 8;
		len |= input.read();
		// runs are always one off
		len += 1;

		// extract the number of bytes occupied by base
		int thirdByte = input.read();
		int bw = (thirdByte >>> 5) & 0x07;
		// base width is one off
		bw += 1;

		// extract patch width
		int pwo = thirdByte & 0x1f;
		int pw = FixedBitSizes.ordinalToFixedBits(pwo);

		// read fourth byte and extract patch gap width
		int fourthByte = input.read();
		int pgw = (fourthByte >>> 5) & 0x07;
		// patch gap width is one off
		pgw += 1;

		// extract the length of the patch list
		int pl = fourthByte & 0x1f;

		// read the next base width number of bytes to extract base value
		byte[] buffer = new byte[bw];
		for (int i = 0; i < bw; i++) {
			buffer[i] = (byte) input.read();
		}

		long base = Utils.bytesToLongBE(buffer);

		// calculate the total bytes of data blob and patch blob
		// data blob length = fixed width bits (fb) * run length (len)
		// patch blob length = patch length * (patch width + patch gap width)
		int blobBytes = BitPackWriter.getTotalBytesRequired(len, fb);
		int patchBlobBytes = BitPackWriter.getTotalBytesRequired(pl, pw + pgw);

		// create buffer space and unpack the bit packed data blob
		buffer = new byte[blobBytes];
		for (int i = 0; i < blobBytes; i++) {
			buffer[i] = (byte) input.read();
		}

		// create buffer space and unpack the bit packed patch blob
		byte[] patchBuffer = new byte[patchBlobBytes];
		for (int i = 0; i < patchBlobBytes; i++) {
			patchBuffer[i] = (byte) input.read();
		}

		// unpack the patch blob
		long[] unpackedPatch = BitPackReader.unpack(patchBuffer, pl, pw + pgw);

		// apply the patch directly when decoding the packed data
		int patchIdx = 0;
		long currGap = 0;
		long currPatch = 0;
		currGap = unpackedPatch[patchIdx] >>> pw;
		currPatch = unpackedPatch[patchIdx] & ((1 << pw) - 1);
		long actualGap = 0;

		// special case: gap is >255 then patch value will be 0.
		// if gap is <=255 then patch value cannot be 0
		while (currGap == 255 && currPatch == 0) {
			actualGap += 255;
			patchIdx++;
			currGap = unpackedPatch[patchIdx] >>> pw;
			currPatch = unpackedPatch[patchIdx] & ((1 << pw) - 1);
		}
		// add the left over gap
		actualGap += currGap;

		// unpack data blob, patch it (if required), add base and the zigzag
		// decode the value to get final result
		long[] unpacked = BitPackReader.unpack(buffer, len, fb);
		for (int i = 0; i < unpacked.length; i++) {
			if (i == actualGap) {
				// apply patch
				long patchedVal = unpacked[i] | (currPatch << fb);

				// add base to patched value and zigzag decode it
				literals[numLiterals++] = Utils.zigzagDecode(base + patchedVal);

				// increment the patch to point to next entry in patch list
				patchIdx++;

				if (patchIdx < pl) {
					// read the next gap and patch
					currGap = unpackedPatch[patchIdx] >>> pw;
					currPatch = unpackedPatch[patchIdx] & ((1 << pw) - 1);
					actualGap = 0;

					// special case: gap is >255 then patch will be 0. if gap is
					// <=255 then patch cannot be 0
					while (currGap == 255 && currPatch == 0) {
						actualGap += 255;
						patchIdx++;
						currGap = unpackedPatch[patchIdx] >>> pw;
						currPatch = unpackedPatch[patchIdx] & ((1 << pw) - 1);
					}
					// add the left over gap
					actualGap += currGap;

					// next gap is relative to the current gap
					actualGap += i;
				}
			} else {
				// no patching required. add base to unpacked value and zigzag
				// decode to get final value
				literals[numLiterals++] = Utils.zigzagDecode(base + unpacked[i]);
			}
		}

	}

	private void readDirectValues(int firstByte) throws IOException {

		// extract the number of fixed bits
		int fbo = (firstByte >>> 1) & 0x1f;
		int fb = FixedBitSizes.ordinalToFixedBits(fbo);

		// extract the run length
		int len = (firstByte & 0x01) << 8;
		len |= input.read();
		// runs are one off
		len += 1;

		// read the bit packed data blob
		int bytesToRead = BitPackWriter.getTotalBytesRequired(len, fb);

		// create buffer space to fill up the blob and unpack the values
		byte[] buffer = new byte[bytesToRead];
		for (int i = 0; i < bytesToRead; i++) {
			buffer[i] = (byte) input.read();
		}

		// write the unpacked values and zigzag decode to result buffer
		long[] unpacked = BitPackReader.unpack(buffer, len, fb);
		for (int i = 0; i < unpacked.length; i++) {
			literals[numLiterals++] = Utils.zigzagDecode(unpacked[i]);
		}
	}

	private void readShortRepeatValues(int firstByte) throws IOException {

		// read the number of bytes occupied by the value
		int size = (firstByte >>> 3) & 0x07;
		// #bytes are one off
		size += 1;

		// read the run length
		int len = firstByte & 0x07;
		// run lengths values are stored only after MIN_REPEAT value is met
		len += NewRunLengthIntegerWriter.MIN_REPEAT;

		// read the repeated value which is store using fixed bytes
		byte[] buffer = new byte[size];
		for (int i = 0; i < size; i++) {
			buffer[i] = (byte) input.read();
		}

		long val = Utils.bytesToLongBE(buffer);
		val = Utils.zigzagDecode(val);

		// repeat the value for length times
		for (int i = 0; i < len; i++) {
			literals[numLiterals++] = val;
		}
	}

	boolean hasNext() throws IOException {
		return used != numLiterals || input.available() > 0;
	}

	long next() throws IOException {
		long result;
		if (used == numLiterals) {
			numLiterals = 0;
			used = 0;
			readValues();
		}
		result = literals[used++];
		return result;
	}

	void seek(PositionProvider index) throws IOException {
		input.seek(index);
		int consumed = (int) index.getNext();
		if (consumed != 0) {
			// a loop is required for cases where we break the run into two
			// parts
			while (consumed > 0) {
				numLiterals = 0;
				readValues();
				used = consumed;
				consumed -= numLiterals;
			}
		} else {
			used = 0;
			numLiterals = 0;
		}
	}

	void skip(long numValues) throws IOException {
		while (numValues > 0) {
			if (used == numLiterals) {
				numLiterals = 0;
				used = 0;
				readValues();
			}
			long consume = Math.min(numValues, numLiterals - used);
			used += consume;
			numValues -= consume;
		}
	}
}
