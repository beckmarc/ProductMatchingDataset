package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;

/**
 * @author Anna Primpeli
 * Removes duplicate offers. Duplicate offers appear because our extractor might assign different nodeIDs to the same entity.
 * We recognize every unique entity by its pld-identifier-identifiervalue-text.
 * Duplicate offers may appear across files
 */
public class Deduplicator extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	long filteredLines = (long)0.0;

	
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
	
	public void process (File object) throws IOException {
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		HashSet<String> uniqueKeys = new HashSet<String>();
		ArrayList<String> deduplicatedData = new ArrayList<String>();
		long filtered=(long)0.0;
		String line;
		
		while ((line = br.readLine()) != null) {
			
			String lineParts[] = line.split("\\t");
			//keys are pld, identifier value and text
			String domain =  DomainUtil.getPayLevelDomainFromWholeURL(lineParts[2]);
			String key = domain+""+""+lineParts[3]+""+lineParts[4]+""+lineParts[5];
			if (uniqueKeys.contains(key)) {
				filtered++;
				continue;
			}
			else {
				deduplicatedData.add(line);
				uniqueKeys.add(key);
			}
		}
		
		br.close();
		integrateFilteredLines(filtered);
		writeInFile(object.getName(), deduplicatedData);
	}
	
	private synchronized void integrateFilteredLines(long filtered) {
		this.filteredLines += filtered;
		
	}

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_DEDUP.txt",false));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	@Override
	protected void afterProcess() {
		System.out.println("Filtered lines because of duplicated pld-idproperty-idvalue-text: "+filteredLines);
		System.out.println("Deduplicated output saved in: "+outputDirectory);
		System.out.println("DONE");
	}
}
