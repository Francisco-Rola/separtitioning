package thesis;

import org.infinispan.distribution.ch.KeyPartitioner;

public class CustomKeyPartitioner implements KeyPartitioner {

	private int experimentNo;
	private int systemNo;
	
	public CustomKeyPartitioner() {
		
	}

	// aux function to get integer until new char
	private static int getIDfromParam(String key, String param) {
		// String to store results
		String result = "";
		// String to read integer from
		String intValue = key.substring(key.indexOf(param) + 1);
		// Remove extra
		for (int i = 0; i < intValue.length(); i++) {
			if (Character.isDigit(intValue.charAt(i))) {
				result += intValue.charAt(i);
			}
			else {
				break;
			}
		}
		return Integer.parseInt(result);
	}

	@Override
	public int getSegment(Object key) {
		
		this.experimentNo = TPCC.experimentNo;
		this.systemNo = TPCC.systemNo;
		
		String keyString = key.toString();

		// Catalyst TPCC 1w2p
		if (experimentNo == 1 && systemNo == 1) {
			if (keyString.startsWith("8")) {
				System.out.println("Table 8 in the wrong KV-store");
				return 0;
			}				
			else if (keyString.contains("d")) {
				if (getIDfromParam(keyString, "d") <= 5)
					return 0;
				else 
					return 1;
			}
			else {
				return 0;
			}
		}
		// Catalyst TPCC 2w2p
		if (experimentNo == 2 && systemNo == 1) {
			if (keyString.startsWith("8")) {
				System.out.println("Table 8 in the wrong KV-store");
				return 0;
			}
			else if (keyString.contains("w")) {
				if (getIDfromParam(keyString, "w") == 1)
					return 0;
				else 
					return 1;
			}
			else {
				return 0;
			}
		}
		// Catalyst TPCC 1w5p
		if (experimentNo == 3 && systemNo == 1) {
			if (keyString.startsWith("8")) {
				System.out.println("Table 8 in the wrong KV-store");
				return 0;
			}				
			else if (keyString.contains("d")) {
				if (getIDfromParam(keyString, "d") <= 2)
					return 0;
				else if (getIDfromParam(keyString, "d") <= 4)
					return 1;
				else if (getIDfromParam(keyString, "d") <= 6)
					return 2;
				else if (getIDfromParam(keyString, "d") <= 8)
					return 3;
				else if (getIDfromParam(keyString, "d") <= 10)
					return 4;
				else {
					System.out.println("Invalid key being asked for!");
					return 0;
				}
			}
			else {
				return 0;
			}
		}
		
		// Schism TPCC 1w2p
		if (experimentNo == 1 && systemNo == 2) {
			if (keyString.startsWith("8")) {
				System.out.println("Table 8 in the wrong KV-store");
				return 0;
			}				
			else if (keyString.contains("d")) {
				if (getIDfromParam(keyString, "d") <= 6)
					return 0;
				else 
					return 1;
			}
			else {
				// no features in key from weka, run random hashing
				int part = (int) (key.toString().hashCode() % 2);
				return part;
			}
		}
		
		// Schism TPCC 2w2p
		if (experimentNo == 2 && systemNo == 2) {
			if (keyString.startsWith("8")) {
				System.out.println("Table 8 in the wrong KV-store");
				return 0;
			}
			else if (keyString.contains("w")) {
				if (getIDfromParam(keyString, "w") == 1)
					return 0;
				else 
					return 1;
			}
			else {
				// no features in key from weka, run random hashing
				int part = (int) (key.toString().hashCode() % 2);
				return part;
			}
		}
		
		// Schism TPCC 1w5p
		if (experimentNo == 3 && systemNo == 2) {
			if (keyString.startsWith("8")) {
				System.out.println("Table 8 in the wrong KV-store");
				return 0;
			}				
			else if (keyString.contains("d")) {
				if (getIDfromParam(keyString, "d") <= 2)
					return 0;
				else if (getIDfromParam(keyString, "d") <= 4)
					return 1;
				else if (getIDfromParam(keyString, "d") <= 6)
					return 2;
				else if (getIDfromParam(keyString, "d") <= 9)
					return 3;
				else if (getIDfromParam(keyString, "d") <= 10)
					return 4;
				else {
					System.out.println("Invalid key being asked for!");
					return 0;
				}
			}
			else {
				// no features in key from weka, run random hashing
				int part = (int) (key.toString().hashCode() % 2);
				return part;
			}
		}
		
		
		
		// Invalid parameters provided to partitioner
		System.out.println("Invalid parameters provided to partitioner");
		return 0;
	}
}

