package thesis;

import java.util.HashMap;

public class Split {
	
	private String splitParam;
	
	private int lowerBound;
	
	private int upperBound;
	
	private int tableSplit = -1;
	
	public Split(String splitParam, int lowerBound, int upperBound) {
		if (splitParam.contains("#"))
			this.tableSplit = Integer.valueOf(splitParam.substring(1));
		this.splitParam = splitParam;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}

	public String getSplitParam() {
		return splitParam;
	}

	public int getLowerBound() {
		return lowerBound;
	}

	public int getUpperBound() {
		return upperBound;
	}
	
	// method that checks if the current rule comprises this access
	public boolean query(long key, int table, HashMap<String, Integer> features) {
		// check if this is a table split rule
		if (this.tableSplit == table) {			
			if (this.lowerBound <= key && key <= this.upperBound)
				return true;
			else 
				return false;
		}
		// check for input split rule
		int splitFeatureValue = -1;
		if (features.containsKey(splitParam))
			splitFeatureValue = features.get(splitParam);
		
		// if feature does not exist don't check bounds
		if (splitFeatureValue == -1)
			return false;
		
		if (this.lowerBound <= splitFeatureValue && splitFeatureValue <= this.upperBound)
			return true;
		else
			return false;	
	}
	
	// method that prints out a rule
	public void printRule(int part) {
		// table split print
		if (tableSplit != -1) {
			System.out.print("Table split: " + tableSplit);
			System.out.print(" | Lower: " + this.lowerBound + " Upper: " + this.upperBound);
			System.out.print(" | Part: " + part + "\n");
		}
		// input split print
		else {
			System.out.print("Input split: " + splitParam);
			System.out.print(" | Lower: " + this.lowerBound + " Upper: " + this.upperBound);
			System.out.print(" | Part: " + part + "\n");
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + lowerBound;
		result = prime * result + ((splitParam == null) ? 0 : splitParam.hashCode());
		result = prime * result + tableSplit;
		result = prime * result + upperBound;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Split other = (Split) obj;
		if (lowerBound != other.lowerBound)
			return false;
		if (splitParam == null) {
			if (other.splitParam != null)
				return false;
		} else if (!splitParam.equals(other.splitParam))
			return false;
		if (tableSplit != other.tableSplit)
			return false;
		if (upperBound != other.upperBound)
			return false;
		return true;
	}
	
	

	

}
