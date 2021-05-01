package wdc.productcorpus.datacreator.Profiler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.Histogram;
import wdc.productcorpus.util.SortMap;

/**
 * @author Anna Primpeli
 *  Profiling of the offers corpus. The corpus has to have the following structure:
 *  filename \t nodeID \t url \t identifyingproperty \t identifyingpropertyvalue \t textualcontent
 */
public class IDInfoProfiler extends Processor<File> {


	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-binSize", required = true, description = "Number of bins for the value histogram.")
	private Integer bins;
	
	long parsedLines = (long)0.0;
	private HashMap<String, Integer> valuesCount = new HashMap<String,Integer>();
	private HashMap<String, Integer> pldCount = new HashMap<String, Integer>();
	private HashMap<String, Integer> propertyCount = new HashMap<String, Integer>();
	private HashSet<String> uniqueSupervisedProducts = new HashSet<String>();
	private HashMap<String, HashSet<String>> values_distinctPLDs = new HashMap<String, HashSet<String>>();

	
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
		
		long lineCount = (long)0.0;
		HashMap<String, Integer> valuesCount = new HashMap<String,Integer>();
		HashMap<String, Integer> pldCount = new HashMap<String, Integer>();
		HashMap<String, Integer> propertyCount = new HashMap<String, Integer>();
		HashMap<String, HashSet<String>> values_distinctPLDs = new HashMap<String, HashSet<String>>();
		HashSet<String> uniqueSupervisedProducts = new HashSet<String>();
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		String line="";

		while (br.ready()) {
			try {
				line = br.readLine();
				lineCount++;
				String [] lineParts =line.split("\\t");
								
				//countValues
				String value = lineParts[4];
				Integer currentvaluecount = valuesCount.get(value);
				if (null == currentvaluecount) currentvaluecount=0;
				valuesCount.put(value, ++currentvaluecount);
				
				//count pld
				String domain = DomainUtil.getPayLevelDomainFromWholeURL(lineParts[2]);
				Integer currentpldcount = pldCount.get(domain);
				if (null == currentpldcount) currentpldcount=0;
				pldCount.put(domain, ++currentpldcount);
				
				//count property
				String property = lineParts[3];
				Integer currentpropertycount = propertyCount.get(property);
				if (null == currentpropertycount) currentpropertycount=0;
				propertyCount.put(property, ++currentpropertycount);
				
				//count unique plds per value
				HashSet<String> currentpldsforValue = values_distinctPLDs.get(value);
				if (null == currentpldsforValue) currentpldsforValue = new HashSet<String>();
				currentpldsforValue.add(domain);
				values_distinctPLDs.put(value, currentpldsforValue);
				
				//count unique elements with identifiers
				uniqueSupervisedProducts.add(lineParts[1].concat(lineParts[2]));
				

			}
			catch (Exception e){
				System.out.println(e.getMessage());
				System.out.println("Line could not be parsed:"+line);
				continue;
			}
		}
		
		integrateValues(valuesCount);
		integratePLDs(pldCount);
		integrateProperties(propertyCount);
		integratePLDsPerValue(values_distinctPLDs);
		updateLineCount(lineCount);
		updateUniqueNodes(uniqueSupervisedProducts);
		
	}

 

	private synchronized void updateLineCount(long lineCount) {
		this.parsedLines += lineCount;
		
	}
	
	private synchronized void updateUniqueNodes(HashSet<String> uniqueSupervisedProducts) {
		this.uniqueSupervisedProducts.addAll(uniqueSupervisedProducts);
		
	}

	private synchronized void integratePLDsPerValue(HashMap<String, HashSet<String>> values_distinctPLDs) {
		
		for (String value : values_distinctPLDs.keySet()) {
			HashSet<String> plds = this.values_distinctPLDs.get(value);
			if (plds == null) {
				plds = values_distinctPLDs.get(value);
			} else {
				plds.addAll(values_distinctPLDs.get(value));
			}
			this.values_distinctPLDs.put(value, plds);
		}
		
	}
	
	private synchronized void integrateProperties(HashMap<String, Integer> propertyCount) {
		
		for (String prop : propertyCount.keySet()) {
			Integer value = this.propertyCount.get(prop);
			if (value == null) {
				value = propertyCount.get(prop);
			} else {
				value += propertyCount.get(prop);
			}
			this.propertyCount.put(prop, value);
		}
		
	}

	private synchronized void integratePLDs(HashMap<String, Integer> pldCount) {
		for (String pld : pldCount.keySet()) {
			Integer value = this.pldCount.get(pld);
			if (value == null) {
				value = pldCount.get(pld);
			} else {
				value += pldCount.get(pld);
			}
			this.pldCount.put(pld, value);
		}
		
	}

	private synchronized void integrateValues(HashMap<String, Integer> valuesCount) {
		for (String id : valuesCount.keySet()) {
			Integer value = this.valuesCount.get(id);
			if (value == null) {
				value = valuesCount.get(id);
			} else {
				value += valuesCount.get(id);
			}
			this.valuesCount.put(id, value);
		}
		
	}

	@Override
	protected void afterProcess() {
		try{

			// write unique values count
			System.out.println("Write stats about values count");
			BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/values_count.txt",false));
			LinkedHashMap<String, Integer> sorted_valuesCount = SortMap.sortByValue(valuesCount);

			
			for (Map.Entry<String, Integer> value : sorted_valuesCount.entrySet())
				values_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			values_writer.flush();
			values_writer.close();
		
			//write unique plds count
			System.out.println("Write stats about plds count");
			BufferedWriter plds_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/plds_count.txt",false));
			LinkedHashMap<String, Integer> sorted_pldCount = SortMap.sortByValue(pldCount);
			
			for (Map.Entry<String, Integer> value : sorted_pldCount.entrySet())
				plds_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			plds_writer.flush();
			plds_writer.close();
			
			//write count of plds per unique value
			System.out.println("Write stats about count of distinct plds per value");
			BufferedWriter valuesplds_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/valuesplds_count.txt",false));
			
			HashMap<String,Integer> count = transformtoCounts(values_distinctPLDs);
			LinkedHashMap<String, Integer> sorted_valuesPerCountPLD = SortMap.sortByValue(count);
			
			for (Map.Entry<String, Integer> value : sorted_valuesPerCountPLD.entrySet())
				valuesplds_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			valuesplds_writer.flush();
			valuesplds_writer.close();

			//write general stats
			System.out.println("Write general stats");

			BufferedWriter general_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/general_info.txt",false));
			general_writer.write("Total number of parsed lines: "+parsedLines+"\n");
			general_writer.write("Unique nodes with identifiers: "+uniqueSupervisedProducts.size()+"\n");
			general_writer.write("Unique identifier values: "+valuesCount.size()+"\n");
			general_writer.write("Occurrences of the different identifying properties \n");
			
			for (Map.Entry<String, Integer> value : propertyCount.entrySet())
				general_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			general_writer.flush();
			general_writer.close();
						
			//write histograms for values
			System.out.println("Calculate histogram of overlapping values");
			System.out.println("Write histogram");

			Histogram<String> utils = new Histogram<String>("Distribution of overlapping values", true, new FileWriter(outputDirectory.toString()+"/histogram_values.txt",false));
			utils.drawDistr(bins, valuesCount);
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

	private HashMap<String, Integer> transformtoCounts(HashMap<String, HashSet<String>> map) {

		HashMap<String,Integer> transformedMap = new HashMap<String, Integer>();
		
		for(Map.Entry<String, HashSet<String>> e: map.entrySet())
			transformedMap.put(e.getKey(), e.getValue().size());
		
			return transformedMap;
	}
	
	
}
