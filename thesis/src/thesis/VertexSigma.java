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
		String newPhi = rhos.get(rho) + " && !(" + phi + ")";
		rhos.put(rho, newPhi);
	}
	
	public HashMap<String, String> getRhos() {
		return this.rhos;
	}
	
	public VertexSigma(HashSet<String> rhos) {
		for (String rho: rhos) {
			HashSet<String> variables = findVariables(rho);
			String phi = "";
			for (String variable : variables) {
				switch(variable) {
					case "district_id":
						phi += "0 <= district_id < 10 && ";
						break;
					case "warehouse_id":
						phi += "0 <= warehouse_id < 10 && ";
						break;
					case "customer_id":
						phi += "0 <= client_id < 100 && ";
						break;
					case "ol_supply_w_id":
						phi += "0 <= ol_supply_w_id < 10";
						break;
					case "ol_i_id":
						phi += "0 <= ol_i_id < 10";
						break;
					default:
						System.out.println("Missing case -> " + variable);
						break;
				}
			}
			phi = phi.substring(0, phi.length() - 4);
			this.rhos.put(rho, phi);
		}
	}
	
	private static HashSet<String> findVariables(String rho) {
		HashSet<String> variables = new HashSet<>();
		Matcher m = Pattern.compile("\\w+_id").matcher(rho);
		while(m.find()) {
			variables.add(rho.substring(m.start(), m.end()));
			//System.out.println(rho1.substring(m.start(), m.end()));
		}
		return variables;
	}
	
	
	@Override
	public boolean test(Integer t) {		
		return false;
	}


}
