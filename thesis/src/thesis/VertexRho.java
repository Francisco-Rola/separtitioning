package thesis;

import java.util.HashSet;
import java.util.StringJoiner;
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
	
	public VertexRho(VertexRho vertexRho) {
		this.rho = new String(vertexRho.getRho());
		if (vertexRho.getRhoUpdate() != null)
			this.update = new String(vertexRho.getRhoUpdate());
		this.variables = new HashSet<>();
		this.variables.addAll(vertexRho.getVariables());
	}
	
	public String getRho() {
		return this.rho;
	}
	
	public void updateRho(String update) {
		System.out.println(update);
		StringJoiner edgePhiUpdated = new StringJoiner(" || ");
		String[] phiParts = update.split(" \\|\\| ");
		boolean ps = false;
		for (String disjunction: phiParts) {
			StringJoiner conjunctionPhi = new StringJoiner(" && ");
			String[] conjunctionParts = disjunction.split(" && ");
			for (int i = 0; i < conjunctionParts.length; i++) {
				if (!conjunctionParts[i].contains("idV")) {
					conjunctionPhi.add(conjunctionParts[i]);
				}
				else if (conjunctionParts[i].contains("Integers")) {
					ps = true;
					conjunctionPhi.add(conjunctionParts[i].replaceAll("\\| \\w+idV", ""));
				}
			}
			if(!ps) {
				edgePhiUpdated.add("(" + conjunctionPhi.toString());
			}
			else {
				edgePhiUpdated.add(conjunctionPhi.toString() + ")");
			}
			ps = false;
		}
		this.update = edgePhiUpdated.toString();
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
