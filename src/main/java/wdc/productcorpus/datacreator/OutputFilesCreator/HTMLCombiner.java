package wdc.productcorpus.datacreator.OutputFilesCreator;

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
import de.dwslab.dwslib.util.io.InputUtil;

public class HTMLCombiner extends Processor<File>{

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private HashSet<String> htmlObjects = new HashSet<String>();
	private long pagesCounter =0 ;
	
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
		HashSet<String> localHtml = new HashSet<String>();
		String line="";
		
		while ((line = br.readLine()) != null) {
			try {
				localHtml.add(line);
				if (localHtml.size()>10000) {
					writeLocalHTML(localHtml);
					localHtml.clear();
				} 
			}
			catch (Exception e){
				System.out.println(e.getMessage());
				continue;
			}
			
		}
		//write the last part
		writeLocalHTML(localHtml);
		br.close();
	}

	private synchronized void writeLocalHTML(HashSet<String> localHtml) throws IOException {
		this.htmlObjects.addAll(localHtml);
		if (this.htmlObjects.size()>100000) {
			writeInFile();
			this.htmlObjects.clear();
		}
		
	}

	private synchronized void writeInFile() throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter (outputDirectory, true));
		for (String html:this.htmlObjects) {
			writer.write(html+"\n");
			pagesCounter++;
		}
		
		writer.flush();
		writer.close();
	}
	
	@Override
	protected void afterProcess() {
		try {
			writeInFile();
			System.out.println("[HTMLCombiner] Lines appended "+pagesCounter);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
