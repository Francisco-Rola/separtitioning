package thesis;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

import com.wolfram.jlink.KernelLink;
import com.wolfram.jlink.MathLinkException;


public class VertexSigma implements Predicate<Integer>{
	
	private HashMap<String, String> rhos = new HashMap<>();
	
	
	public String getPhi(String rho) {
		return rhos.get(rho);
	}
	
	public void updatePhi(String rho, String phi) {
		
		String phiTrimmed = trimPhi(phi);
		
		String newPhi = rhos.get(rho) + " && !(" + phiTrimmed + ")";
		KernelLink link = MathematicaHandler.getInstance();
		String query = "Simplify[" + newPhi + "]";
		String result = link.evaluateToOutputForm(query, 0);
		rhos.put(rho, result);
	}
	
	private static String trimPhi(String s) {
		String trim = s.replaceAll("(\\s&&\\s)?\\(\\w+_id\\)\\s\\S+\\s\\d+", "");
		String output =  trim.replaceAll("idV", "id");
		return output;
	}
	
	public void removeRho(String rho) {
		rhos.remove(rho);
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rhos == null) ? 0 : rhos.hashCode());
		return result;
	}

	public HashMap<String, String> getRhos() {
		return this.rhos;
	}
	
	public VertexSigma(HashSet<String> rhos) {
		for (String rho: rhos) {
			String rhoUpdated = rho.replaceAll("_","");
			rhoUpdated = rhoUpdated.replaceAll("[\\[\\]]", "");
			if (rhoUpdated.contains("GET")) {				
				rhoUpdated = rhoUpdated.replaceAll("GET.*->10\\)"
						, "irid");
			}
			
			HashSet<String> variables = findVariables(rhoUpdated);
			String phi = "";
			for (String variable : variables) {
				if (variable.equals("districtid")) {
					phi += "0 < districtid <= 10 && ";
				}
				else if (variable.equals("warehouseid")) {
					phi += "0 < warehouseid <= 10 && ";
				}
				else if (variable.equals("customerid")) {
					phi += "0 < customerid <= 10 && ";
				}
				else if (variable.startsWith("olsupplywid")) {
					phi += "0 < " + variable + " <= 10 && ";
				}
				else if (variable.startsWith("oliid")) {
					phi += "0 < " + variable + " <= 10 && ";
				}
				else if (variable.equals("irid")) {
					phi += "0 < irid <= 10 && ";
				}
				else {
					System.out.println("Missing case -> " + variable);
				}
			}
			phi = phi.substring(0, phi.length() - 4);
			this.rhos.put(rhoUpdated, phi);
		}
	}
	
	public HashSet<String> findVariables(String rho) {
		HashSet<String> variables = new HashSet<>();
		Matcher m = Pattern.compile("(\\w+id\\S*)\\s*").matcher(rho);
		while(m.find()) {
			variables.add((m.group(1)));
		}
		return variables;
	}
	
	public void printSigma() {
		for (Map.Entry<String, String> entry: rhos.entrySet()) {
			System.out.println("Rho: " + entry.getKey() + " -> Phi: " + entry.getValue());
		}
		
	}
	
	
	@Override
	public boolean test(Integer t) {		
		return false;
	}


}
