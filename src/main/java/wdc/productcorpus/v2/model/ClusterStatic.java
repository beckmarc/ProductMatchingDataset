package wdc.productcorpus.v2.model;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ClusterStatic {
	
	static ObjectMapper objectMapper = new ObjectMapper();

	public static Cluster parseCluster(String line) {
		try {
			return objectMapper.readValue(line, Cluster.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
