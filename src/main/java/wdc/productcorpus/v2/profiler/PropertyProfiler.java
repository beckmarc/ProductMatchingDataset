package wdc.productcorpus.v2.profiler;

import java.io.BufferedReader;
import java.io.File;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import ldif.runtime.Quad;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;
import wdc.productcorpus.v2.util.QuadParser;

public class PropertyProfiler extends Processor<File>{

	private File outputDirectory; 
	private File inputDirectory;
	private Integer threads;
	
	public PropertyProfiler(File output, File input, Integer threads) {
		this.outputDirectory = output;
		this.inputDirectory = input;
		this.threads = threads;
	}
		
	
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
	
	Map<String, Long> freq_ = new HashMap<String, Long>();
	private long errorCount = 0;
	private long entityCount = 0;	
		
	@Override
	protected void process(File object1) throws Exception {
		
		HashMap<String, Long> freq = new HashMap<String, Long>();
		
		long errorCount = 0;
		long entityCount = 0;	
		
		BufferedReader br = InputUtil.getBufferedReader(object1);
		String currentLine="";
		
		while (br.ready()) {
			try {		
				currentLine = br.readLine(); 
				Entity e = EntityStatic.parseEntity(currentLine);
				increaseCounters(e, freq);
				entityCount++;				
			} catch (Exception e) {
				errorCount++;
			}
		}

		updateCounters(entityCount, errorCount);
		mergeCounters(freq);
		br.close();
	}
	
	private void increaseCounters(Entity e, HashMap<String,Long> freq) {
		for(String att : EntityStatic.getAttributes(e)) {
			long count = freq.containsKey(att) ? freq.get(att) : 0;
			freq.put(att, count + 1);
		}
	}
		
	private void updateCounters(long entityCount, long errorCount) {
		this.entityCount += entityCount;
		this.errorCount += errorCount;	
	}

	private synchronized void mergeCounters(Map<String, Long> freq) {
		for(Map.Entry<String, Long> entry : freq.entrySet()) {
			long count = freq_.containsKey(entry.getKey()) ? freq_.get(entry.getKey()) + entry.getValue() : entry.getValue();
			freq_.put(entry.getKey(), count);
		}
	}
		

	@Override
	protected void afterProcess() {	
		PrintUtils.p("Entities:" + entityCount);
		PrintUtils.p(freq_);
		CustomFileWriter.writeFormattedKeyValuesToFile("profile-properties.txt" , outputDirectory, freq_);
		HashMap<String, String> perc = new HashMap<String, String>();
		DecimalFormat df = new DecimalFormat("#.####");
		df.setRoundingMode(RoundingMode.CEILING);
		for(Map.Entry<String, Long> e : freq_.entrySet()) {
			perc.put(e.getKey(), df.format(((double) e.getValue() / (double) entityCount)));
		}
		CustomFileWriter.writeLineToFile("profile-properties.txt", outputDirectory, "-------------------------Percentages from all entities---------------------------------------------------------------------------");
		CustomFileWriter.writeKeyValuesToFile("profile-properties.txt" , outputDirectory, perc);
	}
}
