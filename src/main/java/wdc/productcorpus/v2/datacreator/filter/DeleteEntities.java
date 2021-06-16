package wdc.productcorpus.v2.datacreator.filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;

public class DeleteEntities extends Processor<File>{
	
	private File outputDirectory; 
	private File inputDirectory;
	private Integer threads;
	
	public DeleteEntities(File output, File input, Integer threads, HashSet<String> badplds) {
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
		String fileName = CustomFileWriter.removeExt(object.getName());
		
		ArrayList<Entity> filteredData = new ArrayList<Entity>();
		String line;
		long eliminatedLines =(long) 0.0;

		while ((line = br.readLine()) != null) {
			
			Entity e = EntityStatic.parseEntity(line);
			String domain = DomainUtil.getPayLevelDomainFromWholeURL(e.url);
			
			if (null != domain && badplds.contains(domain)) eliminatedLines++;
			else filteredData.add(e);
			
			if (filteredData.size()>10000) {
				
				CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "filterPld", filteredData);
				filteredData.clear();
			}
		}
		
		
		
		//write the last part
		CustomFileWriter.writeEntitiesToFile(fileName, outputDirectory, "filterPld", filteredData);
		filteredData.clear();
		integrateElimLines(eliminatedLines);
	}
	
	private synchronized void integrateElimLines(long eliminatedLines) {
		this.eliminatedLines += eliminatedLines;
		
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
