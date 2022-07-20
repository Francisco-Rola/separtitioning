package thesis;

public class Artifact {

	public static void main(String[] args) {
				
		// first parameter must be experiment [1-5]
		String experiment = args[0];
		
		// first experiment
		if (experiment.equals("1")) {			
			// system [Schism/Catalyst/Hashing]
			String system = args[1];
			// Catalyst
			if (system.equals("1")) {
				// workload [1-6]
				String workload = args[2];
				// build execution
				if (workload.equals("1")) {
					// 1w2p
					int noW = 0;
					int noP = 2;
					new GraphBuilder(1, noW, noP);
				}
				else if (workload.equals("2")) {
					// 2w2p
					int noW = 1;
					int noP = 2;
					new GraphBuilder(1, noW, noP);
				}
				else if (workload.equals("3")) {
					// 1w5p
					int noW = 0;
					int noP = 5;
					new GraphBuilder(1, noW, noP);
				}
				else if (workload.equals("4")) {
					// 10w5p
					int noW = 0;
					int noP = 10;
					new GraphBuilder(1, noW, noP);
				}
				else if (workload.equals("5")) {
					// 100w10p
					int noW = 4;
					int noP = 10;
					new GraphBuilder(1, noW, noP);
				}
				else {
					// RUBIS
					int noP = 2;
					new GraphBuilder(2, -1, noP);
				}
			}
			// Schism
			else if (system.equals("2")) {
				
				// workload [1-6]
				String workload = args[2];
				// build execution
				if (workload.equals("1")) {
					// 1w2p
					int noW = 0;
					int noP = 2;
					new Schism(noW, noP);
				}
				else if (workload.equals("2")) {
					// 2w2p
					int noW = 1;
					int noP = 2;
					new Schism(noW, noP);
				}
				else if (workload.equals("3")) {
					// 10w2p
					int noW = 9;
					int noP = 2;
					new Schism(noW, noP);
				}
				else if (workload.equals("4")) {
					// 10w5p
					int noW = 9;
					int noP = 5;
					new Schism(noW, noP);
				}
				else if (workload.equals("5")) {
					// 100w10p
					int noW = 0;
					int noP = 5;
					new Schism(noW, noP);
				}
				else {
					// RUBIS
					int noP = 2;
					new Schism(noP);
				}
			}
		}
	}
}
