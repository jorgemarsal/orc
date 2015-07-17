package org.apache.hadoop.hive.ql.io.orc;

public class OutputBuffer {

	private int writeIdx;
	private byte[] buffer;
	private int numUncompressedValues;
	private int numUValsWritten;
	private int readIdx;
	
	public OutputBuffer(int buffSize) {
		this.buffer = new byte[buffSize];
		this.writeIdx = 0;
		this.setReadIdx(0);
		this.numUncompressedValues = 0;
		this.numUValsWritten = 0;
	}
	
	public void setPosition(int idx) {
		writeIdx = idx;
	}
	
	public int getPosition() {
		return writeIdx - 1;
	}
	
	public void writeByte(byte inp) {
		buffer[writeIdx++] = inp;
	}

	public byte readByte() throws IllegalAccessException {
		if(readIdx > writeIdx) {
			throw new IllegalAccessException("Reached End of Buffer.");
		}
		return buffer[readIdx++];
	}
	
	public int read() throws IllegalAccessException {
		if(readIdx > writeIdx) {
			throw new IllegalAccessException("Reached End of Buffer.");
		}
		return 0xff & buffer[readIdx++];
	}
	
	public int peek() throws IllegalAccessException {
		if(readIdx > writeIdx) {
			throw new IllegalAccessException("Reached End of Buffer.");
		}
		return 0xff & buffer[readIdx];
	}
	
	public byte[] getBuffer() {
		return buffer;
	}
	
	public void setNumUncompressedValues(int numVals) {
		numUncompressedValues = numVals;
	}
	
	public void updateNumUValsWritten(int n) {
		numUValsWritten += n;
	}
	
	public int getNumUValsWritten() {
		return numUValsWritten;
	}
	
	public int getTotalEncodedValues() {
		return numUncompressedValues;
	}

	public int getReadIdx() {
		return readIdx;
	}

	public void setReadIdx(int readIdx) {
		this.readIdx = readIdx;
	}
}
