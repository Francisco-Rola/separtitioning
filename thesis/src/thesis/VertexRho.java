package thesis;

import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VertexRho {
	
	private String rho;
	private String update;
	
	private HashSet<String> variables = null;
	
	public VertexRho(String rho) {
		this.rho = rho;
		
		HashSet<String> variables = new HashSet<>();
		Matcher m = Pattern.compile("(\\w+id\\S*)\\s*").matcher(rho);
		while(m.find()) {
			variables.add((m.group(1)));
		}
		this.variables = variables;
	}
	
	public String getRho() {
		return this.rho;
	}
	
	public void updateRho(String update) {
		this.update = update;
	}
	
	public String getRhoUpdate() {
		return this.update;
	}
	
	public HashSet<String> getVariables() {
		return this.variables;
	}
	
	public void printRho() {
		System.out.println("Rho: " + this.getRho());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rho == null) ? 0 : rho.hashCode());
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
		if (rho == null) {
			if (other.rho != null)
				return false;
		} else if (!rho.equals(other.rho))
			return false;
		return true;
	}
	
	

}
