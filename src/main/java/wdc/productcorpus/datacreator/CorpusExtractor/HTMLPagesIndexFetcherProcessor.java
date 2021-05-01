package wdc.productcorpus.datacreator.CorpusExtractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;

public class HTMLPagesIndexFetcherProcessor extends Processor<Map.Entry<String,HashSet<String>>>{

	private File outputDirectory; 
	private File inputDirectory;
	private Integer threads;
	private HashMap<String, HashSet<String>> urlsToFiles = new HashMap<String,HashSet<String>>();

	private HashSet<String> indexInfo = new HashSet<String>();
	
	public HTMLPagesIndexFetcherProcessor(File output, File input, Integer threads, HashMap<String, HashSet<String>> urlsToFiles) {
		this.outputDirectory = output;
		this.inputDirectory = input;
		this.threads = threads;
		this.urlsToFiles = urlsToFiles;
	}
	
	@Override
	protected List<Map.Entry<String,HashSet<String>>> fillListToProcess() {
		List<Map.Entry<String,HashSet<String>>> files = new ArrayList<Map.Entry<String,HashSet<String>>>();
		for (Map.Entry<String,HashSet<String>> f : urlsToFiles.entrySet()) {		
			files.add(f);			
		}
		return files;
	}
	
	@Override
	protected int getNumberOfThreads() {
		return this.threads;
	}
	
	@Override 
	protected void process(Map.Entry<String,HashSet<String>> file) throws Exception {
		
		BufferedReader br = null;
		HashSet<String> localIndexInfo = new HashSet<String>();
		
		try{
			br = InputUtil.getBufferedReader(new File(inputDirectory+"/"+file.getKey()));
			String line="";

			while ((line = br.readLine()) != null) {
				
				String urlOfLine = line.substring(line.indexOf("\"url\": \"")+8, line.indexOf("\", \"mime\":"));
				if (file.getValue().contains(urlOfLine)) localIndexInfo.add(line);
			}

			br.close();
			
			integrateIndexInfo(localIndexInfo);
		}
		catch (Exception e){
			System.out.println("Exception while searching in file"+inputDirectory+"/"+file.getKey());
			System.out.println(e.getMessage());
			System.exit(0);
		}
				
	}

	private synchronized void integrateIndexInfo(HashSet<String> localIndexInfo) {
		this.indexInfo.addAll(localIndexInfo);
		
	}
	
	@Override
	protected void afterProcess() {
		try {

			// write plds - index files information 
			System.out.println("Write index information");
			BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/pages_indexinfo.txt",false));
			for (String i: indexInfo){
				values_writer.write(i+"\n");
			}
			
			values_writer.flush();
			values_writer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
