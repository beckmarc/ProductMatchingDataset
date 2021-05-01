package wdc.productcorpus.datacreator.Filter;

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

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Filters the offers that come from PLDs which erroneously allocate the same identifier to all their offers.
 */
public class BadPLDsDetect extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = false, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory ; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = false, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = false, description = "Number of threads.")
	private Integer threads;
	
	public static void main(String args[]) {
		
		BadPLDsDetect detect = new BadPLDsDetect();
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
	
	HashMap<String,Integer> valuesPerDomain_ = new HashMap<String,Integer>();
	HashMap<String,HashMap<String,Integer>> frequencyOfValuesPerDomain_ = new HashMap<String,HashMap<String,Integer>>();
	HashSet<String> erroneousplds = new HashSet<String>();


	@Override
	protected void process(File object) throws Exception {
		
		DataImporter importData = new DataImporter();
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		HashMap<String,Integer> valuesPerDomain = new HashMap<String,Integer>();
		HashMap<String,HashMap<String,Integer>> frequencyOfValuesPerDomain = new HashMap<String,HashMap<String,Integer>>();
		
		String line;

		while ((line = br.readLine()) != null) {
			
			//String []lineParts= line.split("\\t");
			
			//String idValue =lineParts[4];			
			//String domain = DomainUtil.getPayLevelDomainFromWholeURL(lineParts[2]);
			
			JSONObject json = new JSONObject(line);
			String url = json.getString("url"); 
			String domain = DomainUtil.getPayLevelDomainFromWholeURL(url);
			
			JSONArray identifiers = (JSONArray) json.get("identifiers");
			HashMap<String, HashSet<String>> identifier_values = importData.getValuesFromJSONArray(identifiers, true);
			
			for (HashSet<String> values: identifier_values.values()) {
				for (String idValue: values){
					//increase the general counter
					Integer currentGenCounter = valuesPerDomain.get(domain);
					if (null == currentGenCounter) currentGenCounter=0;
					currentGenCounter++;
					valuesPerDomain.put(domain, currentGenCounter);
					
					//increase the value specific counter
					HashMap<String,Integer> valuesOfDomain = frequencyOfValuesPerDomain.get(domain);
					if (null == valuesOfDomain) valuesOfDomain = new HashMap<String,Integer>();
					Integer valueFrequency = valuesOfDomain.get(idValue);
					if (null == valueFrequency) valueFrequency =0;
					valueFrequency++;
					valuesOfDomain.put(idValue, valueFrequency);
					frequencyOfValuesPerDomain.put(domain, valuesOfDomain);
					
				}
				
			}
			
			
			
		}
		
		integrateValuesPerDomain(valuesPerDomain);
		integratefrequencyOfValuesPerDomain(frequencyOfValuesPerDomain);
	}

	private synchronized void integratefrequencyOfValuesPerDomain(
			HashMap<String, HashMap<String, Integer>> frequencyOfValuesPerDomain) {
		
		for (String f: frequencyOfValuesPerDomain.keySet()) {
			HashMap<String, Integer> currentMap = frequencyOfValuesPerDomain_.get(f);
			
			if (null == currentMap) {
				currentMap = frequencyOfValuesPerDomain.get(f);
			}
			else {
				for (String v: frequencyOfValuesPerDomain.get(f).keySet()) {
					
					Integer value = currentMap.get(v);
					if (value == null) {
						value = frequencyOfValuesPerDomain.get(f).get(v);
					} else {
						value += frequencyOfValuesPerDomain.get(f).get(v);
					}
					currentMap.put(v, value);
				}
			}
			frequencyOfValuesPerDomain_.put(f, currentMap);
		}
		
	}

	private synchronized void integrateValuesPerDomain(HashMap<String, Integer> valuesPerDomain) {
		
		for (String v : valuesPerDomain.keySet()) {
			Integer value = valuesPerDomain_.get(v);
			if (value == null) {
				value = valuesPerDomain.get(v);
			} else {
				value += valuesPerDomain.get(v);
			}
			valuesPerDomain_.put(v, value);
		}
		
	}
	
	@Override
	protected void afterProcess() {
				
		System.out.println("Search for erroneous plds");
		
		for (Map.Entry<String, Integer> domain:valuesPerDomain_.entrySet()) {
			Integer occurrencesofDomain = domain.getValue();
			HashMap<String,Integer> valuesInDomain = frequencyOfValuesPerDomain_.get(domain.getKey());
			
			for (Map.Entry<String, Integer> valueFreq:valuesInDomain.entrySet()) {
				double valueSupport = (double)valueFreq.getValue()/(double)occurrencesofDomain;
				
				if (valueSupport>0.9 && occurrencesofDomain>=5) {
					System.out.println("Value "+valueFreq.getKey()+" occurs constantly in domain "+domain.getKey()+"("+valueFreq.getValue()+"/"+occurrencesofDomain+")");					
					erroneousplds.add(domain.getKey());
				}
			}
		}
		
		System.out.println("Found "+erroneousplds.size()+" plds misusing identifier values. Rewrite only clean data.");
		
		//now eliminate
		BadPLDsFilter filter =new BadPLDsFilter(outputDirectory, inputDirectory, threads, erroneousplds);
		filter.process();
	}
}
