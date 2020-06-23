package thesis;

import com.wolfram.jlink.*;

public class GraphBuilder {

	public static void main(String[] args) {
		
		System.out.println("Running");
		
		KernelLink ml = null;
        try {
        	String jLinkDir = "/usr/local/Wolfram/Mathematica/12.1/SystemFiles/Links/JLink";
            System.setProperty("com.wolfram.jlink.libdir", jLinkDir);
            ml = MathLinkFactory.createKernelLink("-linkmode launch -linkname 'math -mathlink'");
        } catch (MathLinkException e) {
            System.out.println("Fatal error opening link: " + e.getMessage());
            return;
        }

        try {
            // Get rid of the initial InputNamePacket the kernel will send
            // when it is launched.
            ml.discardAnswer();

            ml.evaluate("Solve(x + ((y * 100) + (z * 10000)) = 100 && x 10 & y < 10 && z < 3000, {x,y,z}, Integers)");
         
            ml.waitForAnswer();
            int result = ml.getInteger();
            
            System.out.println(result);

            
            
            

        } catch (MathLinkException e) {
            System.out.println("MathLinkException occurred: " + e.getMessage());
        } finally {
            ml.close();
        }

	}
}