package wdc.productcorpus.v2.cluster;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.Histogram;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;


public class OfferMergerForSampler extends Processor<File> {
	
	public OfferMergerForSampler(File outputDirectory, File inputCorpusDir, Integer threads,
			HashMap<String, Entity> mappedEntities, HashMap<String, Entity> mappedEntities2, Set<Integer> clusterIds) {
		super();
		this.outputDirectory = outputDirectory;
		this.inputCorpusDir = inputCorpusDir;
		this.threads = threads;
		this.mappedEntities = mappedEntities;
		this.mappedEntities2 = mappedEntities2;
		this.clusterIds = clusterIds;
	}

	public OfferMergerForSampler() {
		// TODO Auto-generated constructor stub
	}

	private File outputDirectory; 	
	private File inputCorpusDir;
	private Integer threads;
	
	private HashMap<String, Entity> mappedEntities = new HashMap<String, Entity>();
	private HashMap<String, Entity> mappedEntities2 = new HashMap<String, Entity>();
	
	private Set<Integer> clusterIds = new HashSet<Integer>();
	
	private Map<Integer, ArrayList<Entity>> processedEntities = Collections.synchronizedMap(new HashMap<Integer, ArrayList<Entity>>());
	private Map<Integer, ArrayList<Entity>> processedEntities2 = Collections.synchronizedMap(new HashMap<Integer, ArrayList<Entity>>());
	
	private Map<Integer, ArrayList<Entity>> entitiesPerCluster = Collections.synchronizedMap(new HashMap<Integer, ArrayList<Entity>>());
	
	
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		if (inputCorpusDir.isDirectory()) {
			for (File f : inputCorpusDir.listFiles()) {
				if (!f.isDirectory()) {
					files.add(f);
				}
			}
		}
		return files;
	}
	
	
	@Override
	protected int getNumberOfThreads() {
		return this.threads;
	}
	

	@Override
	protected void process(File object) throws Exception {	
		BufferedReader br = InputUtil.getBufferedReader(object);
		String line;
		while ((line = br.readLine()) != null) {	
			Entity e = EntityStatic.parseEntity(line); // entity from the big corpus with all attributes
			if(mappedEntities.get(getKey(e)) != null) {
				e.cluster_id = mappedEntities.get(getKey(e)).cluster_id;
				if(!processedEntities.containsKey(e.cluster_id))
				processedEntities.put(e.cluster_id, new ArrayList<Entity>());
				processedEntities.get(e.cluster_id).add(e);
			}
			if(mappedEntities2.get(getKey(e)) != null) {
				e.cluster_id = mappedEntities2.get(getKey(e)).cluster_id;
				if(!processedEntities2.containsKey(e.cluster_id))
				processedEntities2.put(e.cluster_id, new ArrayList<Entity>());
				processedEntities2.get(e.cluster_id).add(e);
			}
			if(clusterIds.contains(e.cluster_id)) {
				if(!entitiesPerCluster.containsKey(e.cluster_id))
				entitiesPerCluster.put(e.cluster_id, new ArrayList<Entity>());
				entitiesPerCluster.get(e.cluster_id).add(e);
			}
			

		}

	}
	
	private String getKey(Entity e) {
		return e.nodeId + "\t" + e.url;
	}
	
	@Override
	protected void afterProcess() {
		HashMap<String,Integer> offersPerClusterN = new HashMap<String,Integer>();
		HashMap<String,Integer> offersPerClusterB = new HashMap<String,Integer>();
		for(Map.Entry<Integer, ArrayList<Entity>> e : entitiesPerCluster.entrySet()) {
			if(processedEntities.containsKey(e.getKey())) {
				offersPerClusterN.put(String.valueOf(e.getKey()), e.getValue().size());
			}
			else {
				offersPerClusterB.put(String.valueOf(e.getKey()), e.getValue().size());
			}
			
		}
		try {
			
			CustomFileWriter.writePairsToFile("sampler-normal", outputDirectory, "", processedEntities);
			CustomFileWriter.writePairsToFile("sampler-bigcluster", outputDirectory, "", processedEntities2);
			CustomFileWriter.writePairsToFile("sampler-entities", outputDirectory, "", entitiesPerCluster);
			// distribution for normal cluster (2 < size < 80)
			Histogram<String> draw = new Histogram<String>("Distribution of offers per cluster normal" , true, new FileWriter (new File (outputDirectory+"/sampler-normal-opc.txt")));
			draw.drawDistr(1, offersPerClusterN);
			// distribution for normal cluster (80 < size)
			Histogram<String> drawB = new Histogram<String>("Distribution of offers per cluster normal" , true, new FileWriter (new File (outputDirectory+"/sampler-big-opc.txt")));
			drawB.drawDistr(1, offersPerClusterB);
			System.out.println("Finished");
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
}
