package wdc.productcorpus.v2.cluster;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;


public class OfferSampler extends Processor<File> {
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in" }, required = true, description = "File that contains the offer mappings to clusters", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-offers" }, description = "Offers per Cluster")
	private int offersPerCluster = 2;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private Map<Integer, ArrayList<Entity>> entitiesPerCluster = Collections.synchronizedMap(new HashMap<Integer, ArrayList<Entity>>());
	private List<Entity> pairs = Collections.synchronizedList(new ArrayList<Entity>());
	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		for (File f : inputDirectory.listFiles()) {
			if (!f.isDirectory()) {
				files.add(f);
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
		String fileName = CustomFileWriter.removeExt(object.getName());
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		String line;
		while ((line = br.readLine()) != null) {	
			
			if(pairs.size() > 2000) {
				break;
			}
			
			Entity e = EntityStatic.parseEntity(line); // entity from the big corpus with all attributes
			
			if(entitiesPerCluster.containsKey(e.cluster_id)) {
				ArrayList<Entity> offers = entitiesPerCluster.get(e.cluster_id); // get the offer
				offers.add(e); // add the newly found offer
				if(offers.size() == offersPerCluster) { // desired cluster size reached
					pairs.addAll(offers); // add to the pairs
					entitiesPerCluster.remove(e.cluster_id);
				}			
			}
			else {
				ArrayList<Entity> entities = new ArrayList<Entity>();
				entities.add(e);
				entitiesPerCluster.put(e.cluster_id, entities);
			}
			
		}
		
	}
	
	private String getKey(Entity e) {
		return e.nodeId + "\t" + e.url;
	}
	
	@Override
	protected void afterProcess() {
		try {
			CustomFileWriter.writePairsToFile("pairs", outputDirectory, new ArrayList<Entity>(pairs));
			PrintUtils.p("Printed " + pairs.size()/2 + "pairs");
			System.out.println("Finished");
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
}
