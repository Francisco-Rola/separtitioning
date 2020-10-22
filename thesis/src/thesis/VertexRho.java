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
	// set of variables comprised by this rho
	private HashSet<String> variables = null;
	
	// default vertex rho constructor, receives string as input
	public VertexRho(String rho) {
		this.rho = rho;
		
		HashSet<String> variables = new HashSet<>();
		Matcher m = Pattern.compile("(\\w+id\\S*)\\s*").matcher(rho);
		while(m.find()) {
			variables.add((m.group(1)));
		}
		this.variables = variables;
	}
	
	// rho constructor for deep copy purpose
	public VertexRho(VertexRho vertexRho) {
		this.remote = vertexRho.isRemote();
		this.rho = new String(vertexRho.getRho());
		if (vertexRho.getRhoUpdate() != null)
			this.update = new String(vertexRho.getRhoUpdate());
		this.variables = new HashSet<>();
		this.variables.addAll(vertexRho.getVariables());
	}
	
	// get string formula associated with this vertex rho
	public String getRho() {
		return this.rho;
	}
	
	// method used to split a rho based on a table range split
	public void splitRho(String split) {
		// check if there is a previous update to concatenate
		if (update != null) {	
			this.update = "(" + this.update + ") && " + this.rho.substring(this.rho.indexOf(">") + 1) + split;
		}
		else {
			this.update = this.rho.substring(this.rho.indexOf(">") + 1) + split;
		}
		// simplify rho update to aid future queries
		simplifyRho(split);
				
	}

	// method used to update a rho upon detecting a remote access, ensures disjointness
	public void updateRho(String update) {
		StringJoiner edgePhiUpdated = new StringJoiner(" || ");
		// split update on each disjunction
		String[] phiParts = update.split(" \\|\\| ");
		// check if parenthesis missing
		boolean ps = false;
		// iterate over disjunctions
		for (String disjunction: phiParts) {
			StringJoiner conjunctionPhi = new StringJoiner(" && ");
			// split conjunctions within disjunction
			String[] conjunctionParts = disjunction.split(" && ");
			// iterate over conjunctions
			for (int i = 0; i < conjunctionParts.length; i++) {
				// if conjunction does not contain idGV it is from the new vertex
				if (!conjunctionParts[i].contains("idGV")) {
					conjunctionPhi.add(conjunctionParts[i]);
				}
				// case for Mathematica compressions
				else if (conjunctionParts[i].contains("Integers")) {
					// need to add parenthesis in this case
					ps = true;
					// remove all components related to old vertex, those solutions are not important
					conjunctionPhi.add(conjunctionParts[i].replaceAll("\\| \\w+idV", ""));
				}
			}
			// if there is only one solution no need to add parenthesis
			if (phiParts.length == 1)
				edgePhiUpdated.add(conjunctionPhi.toString());
			else if(ps) {
				edgePhiUpdated.add("(" + conjunctionPhi.toString());
			}
			else {
				edgePhiUpdated.add(conjunctionPhi.toString() + ")");
			}
			ps = false;
		}
		// check if there is update to concatenate
		if (this.update == null) {
			this.update = "!(" + edgePhiUpdated.toString() + ")";
		}
		else {
			this.update = "(" + this.update + ") && !(" + edgePhiUpdated.toString() + ")";
		}
		// simplify rho update to aid further queries
		simplifyRho(edgePhiUpdated.toString());
	}
	
	// method used to simplify a rho update formula
	public void simplifyRho(String update) {
		// get mathematica link
		KernelLink link = MathematicaHandler.getInstance();
		// simplify rho update expression for further computations
		this.update = link.evaluateToOutputForm("FullSimplify[" + this.update + "]", 0);
		// if there is an error, check what went wrong
		if (this.update.equals("$Failed")) {
			System.out.println(update);
			System.out.println("Failed rho update ->" + update);
		}
	}
	
	// sets a rho as remote
	public void setRemote() {
		this.remote = true;
	}

	// checks if a rho is remote
	public boolean isRemote() {
		return this.remote;
	}
	
	// obtain rho update string
	public String getRhoUpdate() {
		return this.update;
	}
	
	// obtain rho variables
	public HashSet<String> getVariables() {
		return this.variables;
	}
	
	// debug method, prints rho fields
	public void printRho() {
		System.out.println("Rho: " + this.getRho());
		System.out.println("Rho update: " + this.getRhoUpdate());
		System.out.println("Remote: " + this.isRemote());
	}

	// automatically generated hash code for vertex rho
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (remote ? 1231 : 1237);
		result = prime * result + ((rho == null) ? 0 : rho.hashCode());
		result = prime * result + ((update == null) ? 0 : update.hashCode());
		result = prime * result + ((variables == null) ? 0 : variables.hashCode());
		return result;
	}
	
	// automatically generated equals for vertex rho
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VertexRho other = (VertexRho) obj;
		if (remote != other.remote)
			return false;
		if (rho == null) {
			if (other.rho != null)
				return false;
		} else if (!rho.equals(other.rho))
			return false;
		if (update == null) {
			if (other.update != null)
				return false;
		} else if (!update.equals(other.update))
			return false;
		if (variables == null) {
			if (other.variables != null)
				return false;
		} else if (!variables.equals(other.variables))
			return false;
		return true;
	}
	
	

}
