package org.apache.hadoop.hive.ql.io.orc;

public enum FixedBitSizes {
	ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE, THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN, TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX, TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR;

	/**
	 * For a given fixed bit this function returns the corresponding ordinal
	 * @param n
	 * @return ordinal value
	 */
	public static int fixedBitsToOrdinal(int n) {
		if (n == 0) {
			return ONE.ordinal();
		}

		if (n >= 1 && n <= 24) {
			return n - 1;
		} else if (n > 24 && n <= 26) {
			return TWENTYSIX.ordinal();
		} else if (n > 26 && n <= 28) {
			return TWENTYEIGHT.ordinal();
		} else if (n > 28 && n <= 30) {
			return THIRTY.ordinal();
		} else if (n > 30 && n <= 32) {
			return THIRTYTWO.ordinal();
		} else if (n > 32 && n <= 40) {
			return FORTY.ordinal();
		} else if (n > 40 && n <= 48) {
			return FORTYEIGHT.ordinal();
		} else if (n > 48 && n <= 56) {
			return FIFTYSIX.ordinal();
		} else {
			return SIXTYFOUR.ordinal();
		}
	}

	/**
	 * For a given ordinal this function returns the corresponding fixed bits
	 * @param n
	 * @return fixed bit value
	 */
	public static int ordinalToFixedBits(int n) {

		if (n >= ONE.ordinal() && n <= TWENTYFOUR.ordinal()) {
			return n + 1;
		} else if (n == TWENTYSIX.ordinal()) {
			return 26;
		} else if (n == TWENTYEIGHT.ordinal()) {
			return 28;
		} else if (n == THIRTY.ordinal()) {
			return 30;
		} else if (n == THIRTYTWO.ordinal()) {
			return 32;
		} else if (n == FORTY.ordinal()) {
			return 40;
		} else if (n == FORTYEIGHT.ordinal()) {
			return 48;
		} else if (n == FIFTYSIX.ordinal()) {
			return 56;
		} else {
			return 64;
		}
	}

	/**
	 * For a given fixed bit this function will return the closest available fixed
	 * bit
	 * @param n
	 * @return closest valid fixed bit
	 */
	public static int getClosestFixedBits(int n) {
		if (n == 0) {
			return 1;
		}

		if (n >= 1 && n <= 24) {
			return n;
		} else if (n > 24 && n <= 26) {
			return 26;
		} else if (n > 26 && n <= 28) {
			return 28;
		} else if (n > 28 && n <= 30) {
			return 30;
		} else if (n > 30 && n <= 32) {
			return 32;
		} else if (n > 32 && n <= 40) {
			return 40;
		} else if (n > 40 && n <= 48) {
			return 48;
		} else if (n > 48 && n <= 56) {
			return 56;
		} else {
			return 64;
		}
	}

}
