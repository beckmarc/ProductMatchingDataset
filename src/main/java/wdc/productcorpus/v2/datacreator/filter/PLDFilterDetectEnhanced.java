package wdc.productcorpus.v2.datacreator.filter;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.PrintUtils;

/**
 * @author Marc Becker
 * 
 * Groups all entities of one PLD together and applies the following operations:
 *  
 * 1.) Bad PLD Detect
 * 
 */
public class PLDFilterDetectEnhanced extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = false, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory ; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = false, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = false, description = "Number of threads.")
	private Integer threads;
	
	public static void main(String args[]) {
		
		PLDFilterDetectEnhanced detect = new PLDFilterDetectEnhanced();
		if (args.length>0) {
			detect.inputDirectory = new File (args[0]);
			detect.outputDirectory = new File(args[1]);
			detect.threads = Integer.parseInt(args[2]);
		}
		detect.process();
		
	}
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
	
	// domain -> ( identiferValue -> numberOfOccurences )
	HashMap<String,HashMap<String, Integer>> idValuesPerDomain_ = new HashMap<String,HashMap<String,Integer>>();


	@Override
	protected void process(File object) throws Exception {
		
		BufferedReader br = InputUtil.getBufferedReader(object);
	
		HashMap<String,HashMap<String, Integer>> idValuesPerDomain = new HashMap<String,HashMap<String,Integer>>();
	
		String line;

		while ((line = br.readLine()) != null) {
			
			Entity e = EntityStatic.parseEntity(line);
			String domain = DomainUtil.getPayLevelDomainFromWholeURL(e.url);
			ArrayList<String> identifiers = EntityStatic.getIdentifiers(e);
			String firstIdValue = "";
			boolean flag = true;
			
			
			HashMap<String,Integer> valuesOfDomain = idValuesPerDomain.get(domain);
			if (null == valuesOfDomain) valuesOfDomain = new HashMap<String,Integer>();
			
			for(String idValue : identifiers) {
				if(!firstIdValue.equals(idValue)) { // prevents to count same idValue multiple times on one entity
							
					Integer valueFrequency = valuesOfDomain.get(idValue);
					if (null == valueFrequency) valueFrequency = 0;
					valueFrequency++;
					valuesOfDomain.put(idValue, valueFrequency);
					
				}
				
				if(flag) {
					firstIdValue = idValue;
					flag = false;
				}
			}
			
			idValuesPerDomain.put(domain, valuesOfDomain);
			
			
			
			if(idValuesPerDomain.size() > 100000) {
				integrateidValuesPerDomain(idValuesPerDomain);
				idValuesPerDomain.clear();
			}
			
		}
		
		integrateidValuesPerDomain(idValuesPerDomain);
	}

	private synchronized void integrateidValuesPerDomain(
			HashMap<String, HashMap<String, Integer>> idValuesPerDomain) {
		
		for (String f: idValuesPerDomain.keySet()) {
			HashMap<String, Integer> currentMap = idValuesPerDomain_.get(f);
			
			if (null == currentMap) {
				currentMap = idValuesPerDomain.get(f);
			}
			else {
				for (String v: idValuesPerDomain.get(f).keySet()) {
					
					Integer value = currentMap.get(v);
					if (value == null) {
						value = idValuesPerDomain.get(f).get(v);
					} else {
						value += idValuesPerDomain.get(f).get(v);
					}
					currentMap.put(v, value);
				}
			}
			idValuesPerDomain_.put(f, currentMap);
		}
		
	}
	
	@Override
	protected void afterProcess() {
				
		
		
		HashMap<String, HashSet<String>> valuesPerDomain = new HashMap<String, HashSet<String>>();
		
		for (Map.Entry<String, HashMap<String,Integer>> domain : idValuesPerDomain_.entrySet()) { // for each domain
			for(Map.Entry<String,Integer> entry : domain.getValue().entrySet()) { // for each identifier in a domain
				if(entry.getValue() >= 2) { // only where identifier occure more than once
					HashSet<String> ids = valuesPerDomain.get(domain.getKey());
					if (null == ids) 
					ids = new HashSet<String>();
					ids.add(entry.getKey());
					valuesPerDomain.put(domain.getKey(), ids);
				}
			}
		}
		
		System.out.println("Finished reading input. Starting elimination of identifiers and offers...");
		
//		for(Map.Entry<String, HashSet<String>> entry : valuesPerDomain.entrySet()) {
//			PrintUtils.p("Domain: " + entry.getKey() + "\nValues: " + entry.getValue());
//		}
		
		//now eliminate
		PLDFilterEntitiesEnhanced filter = new PLDFilterEntitiesEnhanced(outputDirectory, inputDirectory, threads, valuesPerDomain);
		filter.process();
	}
}
