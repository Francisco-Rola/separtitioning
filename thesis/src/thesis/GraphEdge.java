package thesis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.wolfram.jlink.KernelLink;

// class that represents a edge in the graph
public class GraphEdge {
	
	// source vertex for the edge, V
	private GraphVertex src;
	// destiny vertex for the edge, GV
	private GraphVertex dest;
	// number of inputs that cause a transaction to be executed remotely
	private int edgeWeight = 0;
	// formula on V that overlaps with a previously existing formula on GV for the given edgePhi
	private String edgeRho = null;
	// set of inputs that cause vertices to overlap hence causing an edge
	private String edgePhi = null;
	
	public GraphEdge(GraphVertex src, GraphVertex dest, String edgeRho, String intersection)  {
		this.src = src;
		this.dest = dest;
		this.edgeRho = edgeRho;
		this.edgePhi = intersection.replaceAll("(\\s&&\\s)?\\(\\w+id\\)\\s\\S+\\s\\d+", "")
						.replaceAll("idV", "id");
		this.edgeWeight = computeEdgeWeight(edgePhi);
		System.out.println("Added edge with weight " + this.edgeWeight );
	}
	
	public GraphVertex getSrc() {
		return src;
	}

	public GraphVertex getDest() {
		return dest;
	}

	public int getEdgeWeight() {
		return this.edgeWeight;
	}
	
	public void printEdge() {
		System.out.println("Edge rho: " + this.edgeRho);
		System.out.println("Edge phi: " + this.edgePhi);
		System.out.println("Edge weight: " + this.edgeWeight);
	}
	
	private int computeEdgeWeight(String edgePhi) {
		
		if (!edgePhi.contains("Integers")) {
			// obtain a mathematica endpoint
			KernelLink link = MathematicaHandler.getInstance();
			// build a length query that calculates length of the overlapping phis
			String query = "Length["+ edgePhi + "]";
			// how many inputs generate remote access 
			String result = link.evaluateToOutputForm(query, 0);
			// parse the output to an integer
			return Integer.parseInt(result);
		}
		else {
			int noSolutions = 0;
			String[] solutions = edgePhi.split("\\|\\|");
			for (String solution: solutions) {
				if (!solution.contains("Integers")) {
					noSolutions++;
					continue;
				}
				else {
					int noSolutionsPartial = 1;
					String[] partialSolutions = solution.split("&&");
					for (String partialSolution: partialSolutions) {
						if (!partialSolution.contains("<")) {
							continue;
						}
						else {
							// check how many solutions in partial solution
							int start = Integer.parseInt(partialSolution.substring(1, partialSolution.indexOf("<") - 1));
							int end = Integer.parseInt(partialSolution.substring(partialSolution.lastIndexOf("<") + 3).trim());
							noSolutionsPartial *= end - start + 1;
						}
					}
					noSolutions += noSolutionsPartial;
					noSolutionsPartial = 0;
				}
				
			}
			return noSolutions;
		}
	}

}
