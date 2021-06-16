package wdc.productcorpus.v2.cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.fasterxml.jackson.databind.ObjectMapper;

import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.datacreator.ListingsAds.DetectListingsAds;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;

public class OfferSamplerEnhanced {
	
	@Parameter(names = { "-out",
	"-outputFile" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDir; 
	
	@Parameter(names = { "-in",
		"-inputFile" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputFileDir;
	
	@Parameter(names = { "-inCorpus" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputCorpusDir;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	static HashMap<Integer, ArrayList<Entity>> entitiesPerCluster = new HashMap<Integer, ArrayList<Entity>>();
	
	public void process() throws Exception {	
		
		readLines(inputFileDir);
		
		PrintUtils.p("Read all offers into memory. Detected: " + entitiesPerCluster.keySet().size() + " clusters."
				+ "\nStarting to count occurences...");
								
		ArrayList<Entity> sample = new ArrayList<Entity>();
		ArrayList<Entity> sample80 = new ArrayList<Entity>();
		HashSet<Integer> clusterIds = new HashSet<Integer>();
		
		Random r = new Random();
		
		int size = entitiesPerCluster.entrySet().size();
		double skip = (double) size / 10000;
		int skipCount = 0;
		
		for(Map.Entry<Integer, ArrayList<Entity>> e : entitiesPerCluster.entrySet()) {
			if(skipCount > 0) {
				skipCount--;
				continue;
			}
			if(e.getValue().size() < 80 && e.getValue().size() >= 2 && sample.size() < 2000) {
				int r1 = r.nextInt(e.getValue().size());
				sample.add(e.getValue().get(r1));
				e.getValue().remove(r1);
				int r2 = r.nextInt(e.getValue().size());
				sample.add(e.getValue().get(r2));
				clusterIds.add(e.getKey());
				skipCount = (int) skip; // skips some iterations
			}
			if(e.getValue().size() >= 80 && sample80.size() < 2000) {
				int r1 = r.nextInt(e.getValue().size());
				sample80.add(e.getValue().get(r1));
				e.getValue().remove(r1);
				int r2 = r.nextInt(e.getValue().size());
				sample80.add(e.getValue().get(r2));
				clusterIds.add(e.getKey());
				skipCount = (int) skip; // skips some iterations
			}
			if(sample.size() > 2000 && sample80.size() > 2000) {
				break;
			}
		}
					
//		CustomFileWriter.writePairsToFile("sample.txt", outputDir, sample);
//		CustomFileWriter.writePairsToFile("sample-80.txt", outputDir, sample80);
		
		HashMap<String, Entity> mappedEntities = new HashMap<String, Entity>();
		for(Entity e : sample) {
			mappedEntities.put(getKey(e), e);
		}
		
		HashMap<String, Entity> mappedEntities2 = new HashMap<String, Entity>();
		for(Entity e : sample80) {
			mappedEntities2.put(getKey(e), e);
		}
		
		PrintUtils.p(mappedEntities.keySet().size());
		OfferMergerForSampler om = new OfferMergerForSampler(outputDir, inputCorpusDir, threads, mappedEntities, mappedEntities2, clusterIds);
		om.process();

//		System.out.println("Finished");
//
//		
//		PrintUtils.p("Wrote output to directory: " + outputDir);
		
	}
	
	private String getKey(Entity e) {
		return e.nodeId + "\t" + e.url;
	}

	private static void readLines(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				if (!f.isDirectory() && f.getName().equals("offers.json")) {
					String line;
					try {
						BufferedReader br = InputUtil.getBufferedReader(f);
						while ((line = br.readLine()) != null) {
							Entity e = EntityStatic.parseEntity(line);
							if(!entitiesPerCluster.containsKey(e.cluster_id))
							entitiesPerCluster.put(e.cluster_id, new ArrayList<Entity>());
							entitiesPerCluster.get(e.cluster_id).add(e);		
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
}
