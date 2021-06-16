package wdc.productcorpus.v2.datacreator.ListingsAds;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.datacreator.Extractor.SupervisedNode;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.v2.util.CustomFileWriter;

public class Filter extends Processor<File> {
	private File outputDirectory; 
	private File inputDirectory;
	private Integer threads;
	
	public Filter(File output, File input, Integer threads, HashSet<String> listingAdsUrls) {
		this.outputDirectory = output;
		this.inputDirectory = input;
		this.threads = threads;
		this.listingAdsUrl = listingAdsUrls;
	}
	HashSet<String> listingAdsUrl = new HashSet<String>();
	
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
			SupervisedNode node = new SupervisedNode(line);
			if(listingAdsUrl.contains(node.getUrl())) {
				eliminatedLines++;
			}
			else {
				filteredData.add(node.nodetoString());
			}
		}
		
		
		//write the last part
		String fileName = CustomFileWriter.removeExt(object.getName());
		CustomFileWriter.writeTextToFile(fileName, outputDirectory, "filterAds", filteredData);
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
