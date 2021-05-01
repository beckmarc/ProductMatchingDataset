package wdc.productcorpus.datacreator.Profiler.SpecTables;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonObject;

import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.datacreator.OutputFilesCreator.OutputOffer;

public class SpecTablesImporter {

	public HashMap<String,SpecificationTable> importSpecTables(File pathToSpecTables) {
		
		HashMap<String,SpecificationTable> tableContentToUrl = new HashMap<String,SpecificationTable>();
		int counterOfUnresolvedDup = 0;

		try {
			ArrayList<File> filesToRead = new ArrayList<File>();
			
			if (pathToSpecTables.isDirectory()) {
				for (int i=0; i < pathToSpecTables.listFiles().length;i++)
					filesToRead.add(pathToSpecTables.listFiles()[i]);
			}
			else filesToRead.add(pathToSpecTables);
			
			for (File f: filesToRead) {
				BufferedReader reader = InputUtil.getBufferedReader(f);
				
				String line="";

				while (reader.ready()) {
					line = reader.readLine();
					JSONObject json = new JSONObject(line);
					
					String url = json.get("url").toString();
					String specTableContent = json.get("specTableContent").toString();
					
					if (tableContentToUrl.containsKey(url)) {
						String existingContent = tableContentToUrl.get(url).getContent();
						if (existingContent != null && !existingContent.isEmpty() && !existingContent.equals(specTableContent))
							counterOfUnresolvedDup++;
					}
					
					JSONObject keyvaluepairs = json.getJSONObject("keyValuePairs");
					
					int size = 0;
					if (null != keyvaluepairs)
						size=keyvaluepairs.length();
					
					
					SpecificationTable table = new SpecificationTable();
					table.setContent(specTableContent);
					table.setKeyValuePairsCounter(size);
					
					tableContentToUrl.put(url, table);
					
				}
			}
			
		}
		catch(Exception e){
			System.out.println("[DataImporter]"+e.getMessage());
		}
		
		System.out.println("Counter of unresolved duplicate tables (different table content): "+counterOfUnresolvedDup);
		System.out.println("Finished loading specification tables");
		return tableContentToUrl;
	}

	public ArrayList<OutputOffer> addTableInfoToOffers(ArrayList<OutputOffer> offers, File specTablesFile) {
		
		HashMap<String,SpecificationTable> tableContentToUrl = importSpecTables(specTablesFile);
		
		for (OutputOffer o : offers) {
			String url = o.getUrl();
			SpecificationTable table = tableContentToUrl.get(url);
			if (!tableContentToUrl.containsKey(url)){
				System.out.println("URL "+url+" was not found in the specification tables dataset.");
				table = new SpecificationTable();
			}
			
			o.setSpecTable(table);
			
		}
		
		return offers;
	}
}
