package thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

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
	
	public void computeVertexWeight() {
		
		KernelLink link = MathematicaHandler.getInstance();
		
		HashMap<String, String> tableAccess = new HashMap<>();
		
		// iterate through all the rhos-phi in the sigma for the vertex
		HashMap<String, String> rhosPhi = sigma.getRhos();
		
		for (Map.Entry<String, String> entry: rhosPhi.entrySet()) {
			// build rho-phi query
			String phi = entry.getValue();
			String rhoFinal = entry.getKey().substring(entry.getKey().indexOf(">") + 1);
			
			// compute function range to know how many values it spawns
			HashSet<String> variables = this.sigma.findVariables(entry.getKey());
			String variableList = "{";
			for (String variable: variables) {
				variableList += variable + ", ";
			}
			variableList = variableList.substring(0, variableList.length() - 2);
			variableList += ", x}";
			
			String query = "Reduce[" + rhoFinal + " == x" + " && " + phi + ", " 
					+ variableList + ", Integers, Backsubstitution -> True]";
			String result = link.evaluateToInputForm(query, 0);

			if (result.equals("False")) {
				// rho fully removed
				System.out.println("Rho empty for input phi, removing");
			}
			else {
				String table = entry.getKey().substring(0, entry.getKey().indexOf(">") - 1);
				if (!tableAccess.containsKey(table)) {
					tableAccess.put(table, result);
				}
				else {
					//System.out.println("Table overlap - need to compute union");
					tableAccess.get(table).concat(" || " + result);
				}
			}
			
		}
		int noItems = 0;
		// parse results, table by table
		for (Map.Entry<String, String> entry: tableAccess.entrySet()) {
			String query = "DeleteDuplicates[" + entry.getValue() + "]";
			String result = link.evaluateToOutputForm(query,0);
			if (!entry.getValue().contains("Element")) {
				noItems += result.split("\\|\\|").length;
			}
			else {
				String[] items = entry.getValue().split("\\|\\|");
				for (String item: items) {
					if (!item.contains("Integers")) {
						noItems++;
						continue;
					}
					else {
						int noItemsPartial = 1;
						String[] partialItems = item.split("&&");
						for (String partialItem: partialItems) {
							if (!partialItem.contains("Inequality")) {
								continue;
							}
							else {
								// check how many solutions in partial solution
								int indexLower = partialItem.indexOf("[");
								int indexUpper = partialItem.indexOf(",");
								String value = partialItem.substring(indexLower + 1, indexUpper);
								int start = Integer.parseInt(value);
								indexLower = partialItem.lastIndexOf(", ");
								indexUpper = partialItem.indexOf("]");
								value = partialItem.substring(indexLower + 2, indexUpper);
								int end = Integer.parseInt(value);
								noItemsPartial *= end - start + 1;
							}
						}
						noItems += noItemsPartial;
						noItemsPartial = 0;
					}
					
				}
			}
		}
		
		this.vertexWeight = noItems;
	}
	
	public void printVertex() {
		System.out.println("Vertex weight: " + this.vertexWeight);
		this.sigma.printSigma();
	}
		
}