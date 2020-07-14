package thesis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
		
		// variables needed to compute vertex weight
		int weight = 0;
		int start = 0;
		int end = 0;
		
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
			variableList += "}";
			
			String query = "FunctionRange[{" + rhoFinal + ", " + phi + "}, " + variableList + ", weight]";
			System.out.println(query);
			
			String result = link.evaluateToOutputForm(query, 0);
			
			if (!result.equals("False")) {
				start = Integer.parseInt(result.substring(0, result.indexOf("<=") - 1));
				end = Integer.parseInt(result.substring(result.lastIndexOf("< ") + 2));
				weight += end - start;
			}
			else {
				System.out.println("Rho fully removed from V");
				// this rho was fully removed from the vertex TODO eventually keep it for spliting options
				this.sigma.getRhos().remove(entry.getKey());
			}
						
			
		}
		
		this.vertexWeight = weight;
	}
		
}