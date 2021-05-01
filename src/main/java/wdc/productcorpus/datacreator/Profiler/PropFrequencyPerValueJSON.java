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
import java.util.Set;
import java.util.StringTokenizer;

import org.json.JSONObject;
import org.json.JSONArray;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.google.common.net.InternetDomainName;

import de.dwslab.dwslib.framework.Processor;
import de.wbsg.loddesc.util.DomainUtils;
import wdc.productcorpus.util.Histogram;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.SortMap;

/**
 * @author Anna Primpeli
 *  Profiling of a single property in tsv/csv files. 
 */
public class PropFrequencyPerValueJSON extends Processor<File> {


	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "File where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-propertyName", required = true, description = "Name of property.")
	private String propertyName;
	
	@Parameter(names = "-multiplyByProp", required = false, description = "Name of property.")
	private String multiplyProp;
	
	
	@Parameter(names = "-histogram", required = false, description = "Build a histogram.")
	private boolean histogram = false;
	
	long parsedLines = (long)0.0;
	private HashMap<String, Integer> valuesCount = new HashMap<String,Integer>();

	
	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		
		if (inputDirectory.isFile()) 
			files.add(inputDirectory);
		else {
			for (File f : inputDirectory.listFiles()) {
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
		
		long lineCount = (long)0.0;
		HashMap<String, Integer> valuesCount = new HashMap<String,Integer>();
		
		BufferedReader reader = InputUtil.getBufferedReader(object);
				
		String line="";

		while (reader.ready()) {
			line = reader.readLine();
			lineCount++;
			JSONObject json = new JSONObject(line);
			
			String value;
			
			if (propertyName.equals("pld") || propertyName.equals("tld"))
				value = json.get("url").toString();
			else
				value = json.get(propertyName).toString();
			
			if (propertyName.equals("identifiers")){
				
				JSONArray identifiers = json.getJSONArray("identifiers");
				for (int i=0; i<identifiers.length();i++) {
					JSONObject id = identifiers.getJSONObject(i);
					Set<String> keys = id.keySet();
					for (String key:keys){
						String idValue = id.get(key).toString().replaceAll("\\]","").replaceAll("\\[","");
						StringTokenizer st = new StringTokenizer(idValue, ",");
						while(st.hasMoreTokens()) {
						   value = st.nextToken().replaceAll("\\s", "");
						   Integer currentvaluecount = valuesCount.get(value);
						   if (null == currentvaluecount) currentvaluecount=0;
						   		valuesCount.put(value, ++currentvaluecount);
						}
					}
			
				}
			}
			else {
				if (propertyName.equals("pld")){
					value = DomainUtils.getDomain(value);
				}
				if (propertyName.equals("tld")){

					String domain = DomainUtils.getDomain(value);
					if (InternetDomainName.isValid(domain)) {
						InternetDomainName internetDomain = InternetDomainName.from(domain);
						if (null != internetDomain.publicSuffix())
							value = internetDomain.publicSuffix().name().toString();
						else value = "notFound";
					}
					else value = "notFound";
					
				}
				
				Integer currentvaluecount = valuesCount.get(value);
				if (null == currentvaluecount) currentvaluecount=0;
				
				if (null != multiplyProp) currentvaluecount= currentvaluecount+Integer.parseInt(json.get(multiplyProp).toString());
				else currentvaluecount++;
				valuesCount.put(value, currentvaluecount);
			}

			
			
		}
		
		integrateValues(valuesCount);
		updateLineCount(lineCount);
		
		reader.close();
			
	}

 

	private synchronized void updateLineCount(long lineCount) {
		this.parsedLines += lineCount;
		
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
			BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputDirectory.toString(),false));
			LinkedHashMap<String, Integer> sorted_valuesCount = SortMap.sortByValue(valuesCount);

			values_writer.write("Parsed lines: "+parsedLines+"\n");
			
			for (Map.Entry<String, Integer> value : sorted_valuesCount.entrySet())
				values_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			values_writer.flush();
			values_writer.close();
	
			if (histogram){
				System.out.println("Build histogram");
				Histogram<String> htg = new Histogram<>("Histogram of frequencies", true, new FileWriter(outputDirectory.toString()+"_HISTOGRAM", false));
				htg.drawDistr(1, valuesCount);
			}
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
