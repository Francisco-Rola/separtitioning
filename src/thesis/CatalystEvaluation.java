package thesis;

public class CatalystEvaluation {
	
	Partitioner partitioner;
	
	public CatalystEvaluation(Partitioner partitioner) {
		this.partitioner = partitioner;
	}
	
	public void evaluateCatalyst(int workload, int noP) {
		// TPCC
		if (workload == 1) {
			TPCCWorkloadGenerator tpcc = new TPCCWorkloadGenerator(GraphBuilder.noW);
			tpcc.evaluateCatalystTPCC(100000, noP, partitioner.getPartLogic());
		}
		// RUBIS 
		else {
			RubisWorkloadGenerator rubis = new RubisWorkloadGenerator();
			rubis.evaluateCatalystRubis(100000, partitioner.getPartLogic());			
		}
	}

}
