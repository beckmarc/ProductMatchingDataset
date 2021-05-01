package wdc.productcorpus.datacreator.Filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
import wdc.productcorpus.util.SortMap;

/**
 * @author Anna Primpeli
 * This filtering step is used to ensure that every identifier appears in different plds.
 * Reads from a file the class labels (of high pld support) and filters the records that contain them
 * Gives stats about the deprecated data
 */
public class ValuesListClassFilter extends Processor<File>{
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-values",
	"-valuesFilterFile" }, required = true, description = "File where the values to be filtered will be read from.", converter = FileConverter.class)
	private File valuesFile;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	private HashSet<String> labels;
	HashMap<String, Integer> deprOffersPerDomain = new HashMap<String,Integer>();
	
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
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		ArrayList<String> matchedLines = new ArrayList<String>();
		HashMap<String, Integer> deprOffersPerDomain = new HashMap<String,Integer>();
		
		String line="";

		while (br.ready()) {
			
			line = br.readLine();
			String [] lineParts =line.split("\\t");
			
			if (labels.contains(lineParts[4])){
				matchedLines.add(line);
			}
			else {
				String url = lineParts[2];
				String domain = DomainUtil.getPayLevelDomainFromWholeURL(url);
				if (null != domain){
					Integer offersOfDomain = deprOffersPerDomain.get(domain);
					if (null == offersOfDomain) offersOfDomain = 0;
					offersOfDomain++;
					deprOffersPerDomain.put(domain, offersOfDomain);
				}
			}
			
			if (matchedLines.size()>10000) {
				writeInFile(object.getName(), matchedLines);
				matchedLines.clear();
			}
		}
		
		//write the last part
		writeInFile(object.getName(), matchedLines);
		matchedLines.clear();
		integrateDeprDomainInfo (deprOffersPerDomain);
		
	}
	
	private synchronized void integrateDeprDomainInfo(HashMap<String, Integer> deprOffersPerDomain) {
		
		for (String domain : deprOffersPerDomain.keySet()) {
			Integer value = this.deprOffersPerDomain.get(domain);
			if (value == null) {
				value = deprOffersPerDomain.get(domain);
			} else {
				value += deprOffersPerDomain.get(domain);
			}
			this.deprOffersPerDomain.put(domain, value);
		}
	}

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_classLabelFilter.txt",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	@Override
	protected void beforeProcess() {
		
		try{
			labels = new HashSet<String>();
			
			BufferedReader reader = new BufferedReader(new FileReader((valuesFile)));

			String line;		
			
			
			while ((line = reader.readLine()) != null) {
				labels.add(line);
			}
			
			reader.close();
			
			System.out.println("Number of labels loaded: "+labels.size());
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
		
	}
	@Override
	protected void afterProcess() {
		try{

			// write unique values count
			System.out.println("Write stats about deprecated offers");
			BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/domains_perDeprecateOffersCount.txt",false));
			LinkedHashMap<String, Integer> deprDomainsSorted = SortMap.sortByValue(deprOffersPerDomain);

			
			for (Map.Entry<String, Integer> value : deprDomainsSorted.entrySet())
				writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			writer.flush();
			writer.close();
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			
		}
	}
}
