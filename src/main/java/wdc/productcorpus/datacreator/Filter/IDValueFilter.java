package wdc.productcorpus.datacreator.Filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.datacreator.OutputFilesCreator.DataImporter;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;
import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Normalizes id values by removing common faulty prefixes like sku and ean and spaces
 * Removes values that after normalization are smaller then 8 characters or bigger than 25, or values that do not include any digit
 */
public class IDValueFilter extends Processor<File>{
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = false, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp");; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = false, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory=  new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\tmp\\input");
	
	@Parameter(names = "-threads", required = false, description = "Number of threads.")
	private Integer threads = 1;
	
	long eliminatedLines = (long)0.0;
	
	private int minimumNumberofClassTokens = 8;
	private int maximumNumberofClassTokens = 25;
	
	public static void main(String args[]) {
		
		IDValueFilter filter = new IDValueFilter();
		if (args.length>0) {
			filter.inputDirectory = new File (args[0]);
			filter.outputDirectory = new File(args[1]);
			filter.threads = Integer.parseInt(args[2]);
		}
		
		filter.process();
	}
	
	
	private ArrayList<String> commonPrefixes = new ArrayList<String>(){{
	    add("sku");
	    add("id");
	    add("item");
	    add("isbn");
	    add("ean");
	    add("upc");
	    add("https");
	    add("stock");
	    add("part");
	    add("style");
	    add("upc");
	    add("product");
	}};
	
	
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
		Integer eliminatedOffers = 0;
		
		DataImporter importOffer = new DataImporter();
		
		ArrayList<String> filteredData = new ArrayList<String>();
		String line;

		while ((line = br.readLine()) != null) {
			
			JSONObject json = new JSONObject(line);
			
			OutputOffer offer = importOffer.jsonToOffer(json);
			
			
			HashMap<String, HashSet<String>> identifier_values = offer.getIdentifiers();
			
			HashMap<String, HashSet<String>> newIdentifier_values = new HashMap<String, HashSet<String>>();
			
			
			for (Map.Entry<String, HashSet<String>> values_ofID : identifier_values.entrySet()) {
				HashSet<String> newValues = new HashSet<String>();
				
				for(String value:values_ofID.getValue()) {
					String value_norm = removePrefixes(value.replaceAll("\\s+",""));
					if (isInterestingClass(value_norm)) newValues.add(value_norm);
				}
				
				if (!newValues.isEmpty()) newIdentifier_values.put(values_ofID.getKey(), newValues);
			}
			
			//write line only if the newIdentifier values are filled
			if (!newIdentifier_values.isEmpty()) {
				
				offer.setIdentifiers(newIdentifier_values);
				filteredData.add(offer.toJSONObject(true).toString());
			}
			else
				eliminatedOffers++;
			
			
			
			if (filteredData.size()>100000) {
				writeInFile(object.getName(), filteredData);
				filteredData.clear();
			}
		}
		//write the last part
		writeInFile(object.getName(), filteredData);
		filteredData.clear();
		
		integrateEliminatedLines(eliminatedOffers);
	}
	
	private String removePrefixes(String value) {
		
		String normValue = value;
		for (String prefix:this.commonPrefixes) {
			if (value.startsWith(prefix))
				normValue = normValue.replaceFirst(prefix, "");
		}
		return normValue;
	}

	private synchronized void integrateEliminatedLines (Integer lines) {
		this.eliminatedLines += lines;
	}

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_filteredIDLabels.txt",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	private boolean isInterestingClass(String classLabel) {
		if (!classLabel.matches(".*\\d+.*") ||classLabel.length()<minimumNumberofClassTokens || classLabel.length()>maximumNumberofClassTokens )
			return false;
		else return true;
	}


	@Override
	protected void afterProcess() {
		try {

			System.out.println("Eliminated lines: "+eliminatedLines);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
