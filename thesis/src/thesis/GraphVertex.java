package thesis;

import java.util.*;

public class GraphVertex {
	
	private int vertexWeight = 0;
	
	private VertexSigma sigma;
	
	public GraphVertex(VertexSigma sigma) {
		this.sigma = sigma;	
	}
	
	public VertexSigma getSigma() {
		return sigma;
	}

}