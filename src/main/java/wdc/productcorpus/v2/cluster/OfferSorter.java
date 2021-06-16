package wdc.productcorpus.v2.cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

public class OfferSorter {
	
	@Parameter(names = { "-out",
	"-outputFile" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputFile ; 
	
	@Parameter(names = { "-in",
		"-inputFile" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputFileDir;
	
	static HashMap<Integer, Integer> occurencesPerCluster = new HashMap<Integer, Integer>();
	static HashMap<Integer, ArrayList<Entity>> entitiesPerCluster = new HashMap<Integer, ArrayList<Entity>>();
	
	public void process() throws Exception {	
		
		readLines(inputFileDir);
		
		PrintUtils.p("Read all offers into memory. Detected: " + entitiesPerCluster.keySet().size() + " clusters."
				+ "\nStarting to count occurences...");
				
		for(Map.Entry<Integer, ArrayList<Entity>> entry : entitiesPerCluster.entrySet()) {
			occurencesPerCluster.put(entry.getKey(), entry.getValue().size());
		}
		
		PrintUtils.p("Counted number of occurences for each cluster. Starting sorting, this may take a while... ");
		
		LinkedHashMap<Integer, Integer> sortedOccurences = sortHashMapByValues(occurencesPerCluster);
		
		PrintUtils.p("Sorting finished. Writing output to file...");
		
		BufferedWriter writer;
		try {
			writer = new BufferedWriter (new FileWriter(outputFile,true));
			ObjectMapper objectMapper = new ObjectMapper();

			for(Integer entry : sortedOccurences.keySet()) {
				for (Entity node : entitiesPerCluster.get(entry))
					writer.write(objectMapper.writeValueAsString(node)+"\n");
			}
			
			writer.flush();
			writer.close();
			
		} catch (IOException e2) {}
		
		PrintUtils.p("Wrote output to file: " + outputFile);
		
		
	}
	


	public static LinkedHashMap<Integer, Integer> sortHashMapByValues(
	        HashMap<Integer, Integer> passedMap) {
	    List<Integer> mapKeys = new ArrayList<>(passedMap.keySet());
	    List<Integer> mapValues = new ArrayList<>(passedMap.values());
	    Collections.sort(mapValues, Collections.reverseOrder());
	    Collections.sort(mapKeys, Collections.reverseOrder());
	
	    LinkedHashMap<Integer, Integer> sortedMap =
	        new LinkedHashMap<>();
	
	    Iterator<Integer> valueIt = mapValues.iterator();
	    while (valueIt.hasNext()) {
	    	Integer val = valueIt.next();
	        Iterator<Integer> keyIt = mapKeys.iterator();
	
	        while (keyIt.hasNext()) {
	            Integer key = keyIt.next();
	            Integer comp1 = passedMap.get(key);
	            Integer comp2 = val;
	
	            if (comp1.equals(comp2)) {
	                keyIt.remove();
	                sortedMap.put(key, val);
	                break;
	            }
	        }
	    }
	    return sortedMap;
	}

	private static void readLines(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				if (!f.isDirectory()) {
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
