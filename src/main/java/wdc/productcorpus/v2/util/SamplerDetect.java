package wdc.productcorpus.v2.util;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.StDevStats;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;

public class SamplerDetect extends Processor<File>{
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory ; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
		
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
	
	public long urlCount = (long)0.0;
	public long entityCount = (long)0.0;
	
	@Override
	protected void process(File object) throws Exception {	
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		ArrayList<Entity> entities = new ArrayList<Entity>();
		
		String line;
		String currentUrl = "";
		
		long urlCount = (long)0.0;
		long entityCount = (long)0.0;

		while ((line = br.readLine()) != null) {	
			Entity e = EntityStatic.parseEntity(line);
			entityCount++;
			
			if (e.url.equals(currentUrl)) { // the identifier value
			    entities.add(e);
			} else {
				if(entities.size() > 0) {
					urlCount++;
				}
				entities.clear();
				entities.add(e);
				currentUrl = e.url;
			}
		}
		
		updateEntityCount(entityCount);
		updateUrlCount(urlCount);		
		
	}
	
	// sync error count with global error count
	private synchronized void updateEntityCount(long count) {
		this.entityCount += count;
	}
	
	// sync error count with global error count
	private synchronized void updateUrlCount(long count) {
		this.urlCount += count;
	}

	
	@Override
	protected void afterProcess() {			
		System.out.println("Amount of Entities " + entityCount);
		System.out.println("Amount of unique URLs: " + urlCount);
	}
	

	


}
