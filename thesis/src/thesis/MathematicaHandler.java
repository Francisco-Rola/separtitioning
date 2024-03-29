package thesis;

import com.wolfram.jlink.KernelLink;
import com.wolfram.jlink.MathLinkException;
import com.wolfram.jlink.MathLinkFactory;

// singleton class that is responsible for keeping a Mathematica endpoint
public class MathematicaHandler {
	// matematica link
	private static KernelLink ml = null;
		
	// default constructor
	private MathematicaHandler() {}
	
	// singleton handler method
	public static KernelLink getInstance() {
		if (ml == null) {
			 try {
		        	String jLinkDir = "/usr/local/Wolfram/Mathematica/12.1/SystemFiles/Links/JLink";
				 	//String jLinkDir = "/usr/local/Wolfram/WolframEngine/13.0/SystemFiles/Links/JLink";
		            System.setProperty("com.wolfram.jlink.libdir", jLinkDir);
		            ml = MathLinkFactory.createKernelLink("-linkmode launch -linkname 'math -mathlink'");
		            ml.discardAnswer();
		            return ml;
		        } catch (MathLinkException e) {
		            System.out.println("Fatal error opening link: " + e.getMessage());
		            return ml;
		        }
		}
		return ml;
	}
}
