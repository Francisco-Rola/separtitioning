package thesis;

import java.util.HashSet;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.wolfram.jlink.KernelLink;

// class that represents a SE formula, maps to a database access
public class VertexRho {
	// flag that denotes if a rho is remote aka in other vertex
	private boolean remote = false;
	// string denoting rho formula
	private String rho = null;
	// update resultant either from logical subtraction to ensure disjointness or splitting
	private String update = null;
	// table associated to the rho
	private String table = null;
	// lower value associated to table split
	private int lowerTableLimit = -1;
	// upper value associated to table split
	private int upperTableLimit = -1;
	// estimated number of remote items that would otherwise be in this rho
	private int remoteItems = 0;
	// estimated number of items in this rho
	private int noItems = 0;
	// probability of tho being used
	private double prob = 1.0;
	// set of variables comprised by this rho
	private HashSet<String> variables = null;
	// value associated to a rho in case it is repeated over the vertex OPTIMIZATION
	private int value = 1;
	
	public int getValue() {
		return this.value;
	}
	
	public void setValue() {
		this.value++;
	}
	
	// default vertex rho constructor, receives string as input
	public VertexRho(String rho) {
		// check if probabilistic rho
		if (rho.contains("#")) {
			// split by delimiter
			String[] probRho = rho.split("#");
			this.rho = probRho[0];
			this.prob = Double.valueOf(probRho[1]) / 100;
		}
		else {
			this.rho = rho;
		}
		HashSet<String> variables = new HashSet<>();
		Matcher m = Pattern.compile("(\\w+id[0-9]*)\\S*\\s*").matcher(rho);
		while(m.find()) {
			variables.add((m.group(1)));
		}
		this.variables = variables;
		
		this.table = rho.substring(0, rho.indexOf(">") - 1);
	}
	
	// rho constructor for deep copy purpose
	public VertexRho(VertexRho vertexRho) {
		this.remote = vertexRho.isRemote();
		this.table = vertexRho.getTable();
		this.prob = vertexRho.getProb();
		this.rho = new String(vertexRho.getRho());
		this.variables = new HashSet<>();
		this.variables.addAll(vertexRho.getVariables());
		this.lowerTableLimit = vertexRho.getLowerTableLimit();
		this.upperTableLimit = vertexRho.getUpperTableLimit();
	}
	
	// get string formula associated with this vertex rho
	public String getRho() {
		return this.rho;
	}
	
	// get table associated to rho
	public String getTable() {
		return this.table;
	}
	
	// get number of remote items in this rho
	public int getRemoteItems() {
		return this.remoteItems;
	}
	
	// setter for number of remote items in this rho
	public void setRemoteItems(int remoteItems) {
		this.remoteItems = remoteItems;
	}
	
	// get number of items in this rho
	public int getNoItems() {
		return this.noItems;
	}
	
	// setter for number of items in this rho
	public void setNoItems(int noItems) {
		this.noItems = noItems;
	}
	
	// method that checks if rho has been table splitted
	public boolean tableSplitExists() {
		if (this.lowerTableLimit != -1)
			return true;
		else
			return false;
	}
	
	// getter for mathematica string containing limits
	public String getTableSplitMath(String rhoQuery) {
		return "" + this.getLowerTableLimit() +
				" <= " + rhoQuery + " <= " + this.getUpperTableLimit();
	}
	
	// get lower table limit
	public int getLowerTableLimit() {
		return this.lowerTableLimit;
	}
	
	// get upper table limit
	public int getUpperTableLimit() {
		return this.upperTableLimit;
	}
	
	// method used to split a rho based on a table range split
	public void splitRho(int lower, int upper) { 
		this.lowerTableLimit = lower;
		this.upperTableLimit = upper;
	}	
	
	// sets a rho as remote
	public void setRemote() {
		this.remote = true;
	}

	// checks if a rho is remote
	public boolean isRemote() {
		return this.remote;
	}
	
	// getter for rho prob
	public Double getProb() {
		return this.prob;
	}
	
	// obtain rho variables
	public HashSet<String> getVariables() {
		return this.variables;
	}
	
	// debug method, prints rho fields
	public void printRho() {
		System.out.println("Rho: " + this.getRho());
		System.out.println("Rho update: " + this.getLowerTableLimit() + "|" + this.getUpperTableLimit());
		System.out.println("Remote: " + this.isRemote());
		System.out.println("Probability: " + this.getProb());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + lowerTableLimit;
		result = prime * result + noItems;
		long temp;
		temp = Double.doubleToLongBits(prob);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (remote ? 1231 : 1237);
		result = prime * result + remoteItems;
		result = prime * result + ((rho == null) ? 0 : rho.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + ((update == null) ? 0 : update.hashCode());
		result = prime * result + upperTableLimit;
		result = prime * result + value;
		result = prime * result + ((variables == null) ? 0 : variables.hashCode());
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
		VertexRho other = (VertexRho) obj;
		if (lowerTableLimit != other.lowerTableLimit)
			return false;
		if (noItems != other.noItems)
			return false;
		if (Double.doubleToLongBits(prob) != Double.doubleToLongBits(other.prob))
			return false;
		if (remote != other.remote)
			return false;
		if (remoteItems != other.remoteItems)
			return false;
		if (rho == null) {
			if (other.rho != null)
				return false;
		} else if (!rho.equals(other.rho))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (update == null) {
			if (other.update != null)
				return false;
		} else if (!update.equals(other.update))
			return false;
		if (upperTableLimit != other.upperTableLimit)
			return false;
		if (value != other.value)
			return false;
		if (variables == null) {
			if (other.variables != null)
				return false;
		} else if (!variables.equals(other.variables))
			return false;
		return true;
	}

	

	
	

}
