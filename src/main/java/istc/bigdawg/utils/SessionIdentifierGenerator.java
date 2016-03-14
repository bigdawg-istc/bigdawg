package istc.bigdawg.utils;

import java.math.BigInteger;
import java.security.SecureRandom;

public enum SessionIdentifierGenerator {
	INSTANCE;

	private SessionIdentifierGenerator() {
	}

	private SecureRandom random = new SecureRandom();

	public String nextRandom26CharString() {
		/**
		 * It generates 130 sudo random bits, then each 5 bits is encoded in
		 * base32, for example, 5 bits that give 0 represent letter A, 5 bits
		 * that given number 31 represent number 7.
		 * 
		 * A 32-character subset of the twenty-six letters A–Z and ten digits
		 * 0–9. Primarily Base32 is used to encode binary data.
		 * 
		 * 128 bits is considered to be cryptographically strong, but each digit
		 * in a base 32 number can encode 5 bits, so 128 is rounded up to the
		 * next multiple of 5. This encoding is compact and efficient, with 5
		 * random bits per character.
		 */
		return new BigInteger(130, random).toString(32);
	}

	public static void main(String[] args) {
		SessionIdentifierGenerator sid = SessionIdentifierGenerator.INSTANCE;
		System.out.println(sid.nextRandom26CharString());
		System.out.println(sid.nextRandom26CharString());
		System.out.println(sid.nextRandom26CharString());
		System.out.println(sid.nextRandom26CharString());
	}

}
