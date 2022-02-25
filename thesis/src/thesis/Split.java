package thesis;

public class Split {
	
	private String splitParam;
	
	private int lowerBound;
	
	private int upperBound;
	
	public Split(String splitParam, int lowerBound, int upperBound) {
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + lowerBound;
		result = prime * result + ((splitParam == null) ? 0 : splitParam.hashCode());
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
		if (upperBound != other.upperBound)
			return false;
		return true;
	}
	
	

}
