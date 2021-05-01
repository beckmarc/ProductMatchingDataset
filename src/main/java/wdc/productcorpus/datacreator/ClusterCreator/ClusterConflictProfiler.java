package wdc.productcorpus.datacreator.ClusterCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Takes as input the corpus offer and check the possible conflicts of the clustered products.
 * The conflicts derive from offers having part of their identifiers similar while other parts dissimilar.
 * Example: o1: gtin13= a, gtin8= b | o2: gtin8=a , mpn= c | o3: gtin13= d, gtin 8 = c
 * The conflicts are a result of identifiers misuse or oversimplification (assignment to an gtin13 to the productID as the vemdor may not know the difference of the different identifying properties) 
 * Outputs a file containing all the conflicted product offers
 */
public class ClusterConflictProfiler extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;

	
	private HashMap<String, ArrayList<String>> loadedEntities = new HashMap<String, ArrayList<String>>();
	
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
	protected void process(File object){
		HashMap<String, ArrayList<String>> tempEntities = new HashMap<String, ArrayList<String>>();

		try{
			BufferedReader br = InputUtil.getBufferedReader(object);
			String line="";

			while ((line = br.readLine()) != null) {
				String [] lineParts =line.split("\\t");
				String key = lineParts[1]+lineParts[2];
				String idValue = lineParts[4];
				
				ArrayList<String> currentValues = tempEntities.get(key);
				if (null == currentValues) currentValues = new ArrayList<String>();
				currentValues.add(idValue);
				tempEntities.put(key, currentValues);
					
			}
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
		
		integrateLoadedEntities(tempEntities);
	}
	
	private synchronized void integrateLoadedEntities(HashMap<String, ArrayList<String>> tempEntities) {
		
		for (String key : tempEntities.keySet()) {
			ArrayList<String> values = this.loadedEntities.get(key);
			if (null == values) values = new ArrayList<String>();
			values.addAll(tempEntities.get(key));

			this.loadedEntities.put(key, values);
		}
		
	}
	@Override
	protected void afterProcess() {
		try{
			System.out.println("Loaded "+loadedEntities.size()+" unique entities. ");
			
			
			HashMap<String, ArrayList<String>> entitiesWithMoreIDProps = new HashMap<String, ArrayList<String>>();
			
			for (Map.Entry<String, ArrayList<String>> e: loadedEntities.entrySet()) {
				if (e.getValue().size()>1) entitiesWithMoreIDProps.put(e.getKey(), e.getValue());
			}
			
			System.out.println("Offers with more than one identifier (duplicate values possible): "+entitiesWithMoreIDProps.size());
			
			System.out.println("Now print the entities with more that one unique identifiers");
			BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/possible_conflicting_entities.txt",false));
			
			Integer conflictingEntities = 0;
			
			for (Map.Entry<String, ArrayList<String>> e: entitiesWithMoreIDProps.entrySet()) {
				HashSet<String> uniqueValues = new HashSet<String> (e.getValue());
				if (uniqueValues.size()>1){
					writer.write(e.getKey()+"\t"+uniqueValues+"\n");
					conflictingEntities++;
				}
			} 
			
			writer.flush();
			writer.close();
			System.out.println("Found "+conflictingEntities+" conflicting entities.");
			
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}

	
}
