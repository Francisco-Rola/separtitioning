package thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.wolfram.jlink.Expr;
import com.wolfram.jlink.ExprFormatException;
import com.wolfram.jlink.KernelLink;

public class GraphVertex {
	
	private int vertexWeight = 0;
	
	private VertexSigma sigma;
	
	public GraphVertex(VertexSigma sigma) {
		this.sigma = sigma;	
	}
	
	public VertexSigma getSigma() {
		return sigma;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sigma == null) ? 0 : sigma.hashCode());
		result = prime * result + vertexWeight;
		return result;
	}
	
	private String getRhoVariables(String rho) {
		HashSet<String> variables = this.sigma.findVariables(rho);
		String variableList = "{";
		for (String variable: variables) {
			variableList += variable + ", ";
		}
		variableList = variableList.substring(0, variableList.length() - 2);
		variableList += ", x}";
		return variableList;
	}
		
	public void computeVertexWeight() {
		// get mathematica endpoint
		KernelLink link = MathematicaHandler.getInstance();
		// table to compute vertex weight
		HashMap<String, HashSet<Integer>> tableAccess = new HashMap<>();
		
		// iterate through all the rhos-phi in the sigma for the vertex
		HashMap<String, String> rhosPhi = sigma.getRhos();
		
		for (Map.Entry<String, String> entry: rhosPhi.entrySet()) {
			// build rho-phi
			String phi = entry.getValue();
			String rho = entry.getKey().substring(entry.getKey().indexOf(">") + 1);
			// obtain rho variables
			String variableList = getRhoVariables(entry.getKey());
			// obtain table number
			String table = entry.getKey().substring(0, entry.getKey().indexOf(">") - 1);
			
		
			if (!tableAccess.containsKey(table)) {
				HashSet<Integer> items = new HashSet<>();
				tableAccess.put(table, items);
			}
			// compute data items in this rho
			String query = "Reduce[" + rho + " == x" + " && " + phi + ", " 
					+ variableList + ", Integers, Backsubstitution -> True]";
			String result = link.evaluateToInputForm(query, 0);
			if (result.equals("False")) {
				System.out.println("Rho fully removed");
				continue;
			}
			// how many values of x can be generated
			if (!result.contains("Integers")) {
				Matcher m = Pattern.compile("x == (\\d+)").matcher(result);
				while (m.find()) {
					tableAccess.get(table).add(Integer.parseInt(m.group(1)));
				}
			}
			else {
				System.out.println(query);
				System.out.println("Unable to compute vertex weight");
			}
			
		}
		for (Map.Entry<String, HashSet<Integer>> entry: tableAccess.entrySet()) {
			this.vertexWeight += entry.getValue().size();
		}
	}
	
	public void printVertex() {
		System.out.println("Vertex weight: " + this.vertexWeight);
		this.sigma.printSigma();
	}
		
}