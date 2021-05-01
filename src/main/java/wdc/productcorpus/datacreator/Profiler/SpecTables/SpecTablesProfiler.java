package wdc.productcorpus.datacreator.Profiler.SpecTables;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.net.InternetDomainName;

import de.wbsg.loddesc.util.DomainUtils;
import wdc.productcorpus.util.InputUtil;


public class SpecTablesProfiler {

	static File specificationTablesFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\SpecificationTables");
	static File outputFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\SpecTablesProfiler\\profile_tables_EnglishCorpus");
	static File offersFile = new File("C:\\Users\\User\\workspace\\TransferLearningDataCreator\\src\\main\\resources\\offersWParentDescClean.json_english.json");
	
	ArrayList<String> selectedPLDs = new ArrayList<String>() {{
		
	}};
	
	public static void main (String args[]) throws IOException {
		
		SpecTablesProfiler profile = new SpecTablesProfiler();
		if (args.length>0) {
			specificationTablesFile = new File(args[0]);
			outputFile = new File(args[1]);
			offersFile = new File(args[2]);
		}
		profile.profileKeyValuePairs();
	}
	
	public void profileKeyValuePairs() throws IOException {
		
		HashMap<Integer, Integer> valuesCount = new HashMap<Integer,Integer>();
		
		//add table information
		SpecTablesImporter loadTables = new SpecTablesImporter();
		HashMap<String,SpecificationTable> tables = loadTables.importSpecTables(specificationTablesFile);
		
		//throw away what does not exist in the corpus
		HashSet<String> urlsInCorpus = getUrlsOfCorpus();
		
		HashSet<String> urlsNonExistinginTableCorpus = new HashSet<String>(urlsInCorpus);
		urlsNonExistinginTableCorpus.removeAll(tables.keySet());

		
		int urls = urlsInCorpus.size();
		int noContent= 0;
		int content =0;
		//profile the property you wish
		for (Map.Entry<String,SpecificationTable> table:tables.entrySet()) {
			String tld = "";

			if (!urlsInCorpus.contains(table.getKey())) continue;
						
			if (!selectedPLDs.isEmpty()) {
				//get tld
				String domain = DomainUtils.getDomain(table.getKey());
				if (InternetDomainName.isValid(domain)) {
					InternetDomainName internetDomain = InternetDomainName.from(domain);
					if (null != internetDomain.publicSuffix())
						tld = internetDomain.publicSuffix().name().toString();
				}
				if(!selectedPLDs.contains(tld)) continue;
			}
			if (table.getValue().getContent().isEmpty() || (tld.isEmpty() && !selectedPLDs.isEmpty()) ) {
				noContent++;
				continue;
			}
			
			content++;
			Integer currentvaluecount = valuesCount.get(table.getValue().getKeyValuePairsCounter());
			if (null==currentvaluecount) currentvaluecount=0;
			currentvaluecount++;
			valuesCount.put(table.getValue().getKeyValuePairsCounter(), currentvaluecount);
	

		}
		
		// write unique values count
		System.out.println("Write stats about values count");
		BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputFile,false));
		LinkedHashMap<Integer, Integer> sorted_valuesCount = new LinkedHashMap<>(valuesCount);

		values_writer.write("Distinct urls: "+urls+"\n");
		values_writer.write("Urls with no information(non existing in the corpus - extraction issue AWS): "+urlsNonExistinginTableCorpus.size()+"\n");
		values_writer.write("Urls with no specification table: "+noContent+"\n");
		values_writer.write("Urls with at least one specification table: "+content+"\n");

		values_writer.write("Key value pairs\tFrequency\n");

	
		for (Map.Entry<Integer, Integer> value : sorted_valuesCount.entrySet())
			values_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
		
		values_writer.flush();
		values_writer.close();
	}

	private HashSet<String> getUrlsOfCorpus() throws JSONException, IOException {
		
		HashSet<String> urls = new HashSet<String>();
		
		BufferedReader reader = InputUtil.getBufferedReader(offersFile);
		
		String line="";
		
		while (reader.ready()) {
			line = reader.readLine();
			JSONObject json = new JSONObject(line);
			
			String value = json.get("url").toString();
			urls.add(value);
		}
		
		return urls;
	}

	
}
