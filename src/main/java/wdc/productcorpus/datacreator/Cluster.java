package wdc.productcorpus.datacreator;

import com.beust.jcommander.JCommander;

import wdc.productcorpus.datacreator.ClusterCreator.ClusterConflictProfiler;
import wdc.productcorpus.datacreator.ClusterCreator.GraphCreator;

public class Cluster {

	public static void main(String args[]) {
		
		Cluster cluster = new Cluster();
		JCommander jc = new JCommander(cluster);

		ClusterConflictProfiler profileConflicts = new ClusterConflictProfiler();
		GraphCreator createGraph = new GraphCreator();
		
		jc.addCommand("profileConflicts", profileConflicts);
		jc.addCommand("createClusters_PajekGraph", createGraph);
		
		try {

			jc.parse(args);
			switch (jc.getParsedCommand()) {
			case "profileConflicts":
				profileConflicts.process();
				break;
			case "createClusters_PajekGraph":
				createGraph.process();
				break;
		}
		} catch (Exception pex) {
			if (jc.getParsedCommand() == null) {
				jc.usage();
			} else {
				switch (jc.getParsedCommand()) {
				case "profileConflicts":
					new JCommander(profileConflicts).usage();
					break;
				case "createClusters_PajekGraph":
					new JCommander(createGraph).usage();
					break;
				
			}
		}
	}
	}
}
