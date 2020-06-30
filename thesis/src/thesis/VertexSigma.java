package thesis;

import java.util.function.Predicate;
import java.util.*;

import com.wolfram.jlink.KernelLink;
import com.wolfram.jlink.MathLinkException;


public class VertexSigma implements Predicate<Integer>{
	
	private HashSet<String> phis = new HashSet<>();
	private HashSet<String> rhos = new HashSet<>();
	
	public HashSet<String> getPhis() {
		return this.phis;
	}
	
	public HashSet<String> getRhos() {
		return this.rhos;
	}
	
	public VertexSigma(HashSet<String> phis, HashSet<String> rhos) {
		this.phis = phis;
		this.rhos = rhos;
	}
	
	private String buildPhi() {
		String finalPhi = "";
		boolean isFirst = true;
		for (String phi: phis) {
			if (!isFirst) {
				finalPhi += " && ";
			}
			finalPhi += phi;
			isFirst = false;
		}
		return finalPhi;
	}
	
	@Override
	public boolean test(Integer t) {
		// build an expression from all the phis combined, obtain vertex domain
		String finalPhi = buildPhi();

		// obtain a mathematica handler to perform operations
		KernelLink link = MathematicaHandler.getInstance();
        try {
        	// ignore first response packet
			link.discardAnswer();
			
			for (String rho : rhos) {
				String query = rho + " && " + finalPhi;
				link.evaluate("Solve(query)");
			}
/*			
			ml.evaluate("2+2");
            ml.waitForAnswer();

            int result = ml.getInteger();
            System.out.println("2 + 2 = " + result);

            // Here's how to send the same input, but not as a string:
            ml.putFunction("EvaluatePacket", 1);
            ml.putFunction("Plus", 2);
            ml.put(3);
            ml.put(3);
            ml.endPacket();
            ml.waitForAnswer();
            result = ml.getInteger();
            System.out.println("3 + 3 = " + result);

            // If you want the result back as a string, use evaluateToInputForm
            // or evaluateToOutputForm. The second arg for either is the
            // requested page width for formatting the string. Pass 0 for
            // PageWidth->Infinity. These methods get the result in one
            // step--no need to call waitForAnswer.
            String strResult = ml.evaluateToOutputForm("4+4", 0);
            System.out.println("4 + 4 = " + strResult); */
			
		} catch (MathLinkException e) {
			System.out.println("Unable to perform sigma evaluation in Mathematica");
			e.printStackTrace();
		}finally {
            link.close();
        }
        
		
		return false;
	}


}
