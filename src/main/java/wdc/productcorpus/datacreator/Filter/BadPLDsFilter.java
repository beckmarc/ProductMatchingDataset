package wdc.productcorpus.datacreator.Filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.json.JSONObject;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;

public class BadPLDsFilter extends Processor<File>{
	
	
	
	private File outputDirectory; 
	private File inputDirectory;
	private Integer threads;
	
	public BadPLDsFilter(File output, File input, Integer threads, HashSet<String> badplds) {
		this.outputDirectory = output;
		this.inputDirectory = input;
		this.threads = threads;
		this.badplds = badplds;
	}
	HashSet<String> badplds = new HashSet<String>();
	
	long eliminatedLines = (long)0.0;

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
		
		ArrayList<String> filteredData = new ArrayList<String>();
		String line;
		long eliminatedLines =(long) 0.0;

		while ((line = br.readLine()) != null) {
			
			JSONObject json = new JSONObject(line);
			String url = json.getString("url"); 
			String domain = DomainUtil.getPayLevelDomainFromWholeURL(url);
		
//			String []lineParts= line.split("\\t");
//			String domain = DomainUtil.getPayLevelDomainFromWholeURL(lineParts[2]);
			if (null != domain && badplds.contains(domain)) eliminatedLines++;
			else filteredData.add(line);
		}
		
		if (filteredData.size()>100000) {
			writeInFile(object.getName(), filteredData);
			filteredData.clear();
		}
		
		//write the last part
		writeInFile(object.getName(), filteredData);
		filteredData.clear();
		integrateElimLines(eliminatedLines);
	}
	
	private synchronized void integrateElimLines(long eliminatedLines) {
		this.eliminatedLines += eliminatedLines;
		
	}

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_filteredbadplds.txt",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
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
