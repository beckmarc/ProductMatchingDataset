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

import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.Histogram;
import wdc.productcorpus.util.SortMap;

/**
 * @author Anna Primpeli
 *  Profiling of the offers corpus. The corpus has to have JSON-OutputOffer format:
 
 */
public class IDInfoProfilerJSON extends Processor<File> {



	private File outputDirectory; 
	private File inputDirectory;
	private Integer threads;
	
	
	public static void main(String args[]) {
		IDInfoProfilerJSON profile = new IDInfoProfilerJSON();
		
		if (args.length>0) {
			profile.outputDirectory =new File(args[0]);
			profile.inputDirectory = new File (args[1]);
			profile.threads = Integer.parseInt(args[2]);
		}
		
		profile.process();
	}
	
	long parsedLines = (long)0.0;
	private HashMap<String, Integer> pldCount = new HashMap<String, Integer>();
	private HashMap<String, Integer> propertyCount = new HashMap<String, Integer>();
	private HashSet<String> uniqueSupervisedProducts = new HashSet<String>();
	private HashSet<String> uniqueValues = new HashSet<String>();


	
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
		HashMap<String, Integer> pldCount = new HashMap<String, Integer>();
		HashMap<String, Integer> propertyCount = new HashMap<String, Integer>();
		HashSet<String> uniqueSupervisedProducts = new HashSet<String>();
		HashSet<String> uniqueIDValues= new HashSet<String>();
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		DataImporter importOffer= new DataImporter();
		
		String line="";

		while ((line=br.readLine())!=null) {
			try {
				lineCount++;
				OutputOffer offer = importOffer.jsonToOffer(new JSONObject(line));
					
				//count pld
				String domain = DomainUtil.getPayLevelDomainFromWholeURL(offer.getUrl());
				Integer currentpldcount = pldCount.get(domain);
				if (null == currentpldcount) currentpldcount=0;
				pldCount.put(domain, ++currentpldcount);
				
				//count property
				for (Map.Entry<String,HashSet<String>> prop:offer.getIdentifiers().entrySet()) {
					Integer currentpropertycount = propertyCount.get(prop.getKey());
					if (null == currentpropertycount) currentpropertycount=0;
					propertyCount.put(prop.getKey(), ++currentpropertycount);
					
					for (String value:prop.getValue())
						uniqueIDValues.add(value);
				}
		
				
				//count unique elements with identifiers
				uniqueSupervisedProducts.add(offer.getNodeID().concat(offer.getUrl()));
				

			}
			catch (Exception e){
				System.out.println(e.getMessage());
				System.out.println("Line could not be parsed:"+line);
				continue;
			}
		}
		
		integratePLDs(pldCount);
		integrateProperties(propertyCount);
		updateLineCount(lineCount);
		updateUniqueNodes(uniqueSupervisedProducts);
		updateUniqueValues(uniqueIDValues);
	}

 

	private void updateUniqueValues(HashSet<String> uniqueIDValues) {
		this.uniqueValues.addAll(uniqueIDValues);
	}

	private synchronized void updateLineCount(long lineCount) {
		this.parsedLines += lineCount;
		
	}
	
	private synchronized void updateUniqueNodes(HashSet<String> uniqueSupervisedProducts) {
		this.uniqueSupervisedProducts.addAll(uniqueSupervisedProducts);
		
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

	

	@Override
	protected void afterProcess() {
		try{

		
			//write unique plds count
			System.out.println("Write stats about plds count");
			BufferedWriter plds_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/plds_count.txt",false));
			LinkedHashMap<String, Integer> sorted_pldCount = SortMap.sortByValue(pldCount);
			
			for (Map.Entry<String, Integer> value : sorted_pldCount.entrySet())
				plds_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			plds_writer.flush();
			plds_writer.close();
			

			//write general stats
			System.out.println("Write general stats");

			BufferedWriter general_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/general_info.txt",false));
			general_writer.write("Total number of parsed lines: "+parsedLines+"\n");
			general_writer.write("Unique nodes with identifiers: "+uniqueSupervisedProducts.size()+"\n");
			general_writer.write("Unique identifier values:"+uniqueValues.size()+"\n");
			general_writer.write("Occurrences of the different identifying properties \n");
			
			for (Map.Entry<String, Integer> value : propertyCount.entrySet())
				general_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			general_writer.flush();
			general_writer.close();
						
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

	
	
}
