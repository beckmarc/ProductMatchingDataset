package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;

public class ValueSearch extends Processor<File> {

	@Parameter(names = { "-in",
	"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the output is written to.", converter = FileConverter.class)
	private File outputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;

	@Parameter(names = "-value", required = true, description = "Value to search.")
	private String value;
	
	private ArrayList<String> matchedLines = new ArrayList<String>();
	

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
		ArrayList<String> matchedLineslocal = new ArrayList<String>();
		
		String line="";

		while (br.ready()) {
			
			line = br.readLine();
			String [] lineParts =line.split("\\t");
			
			if (lineParts[4].equals(value))
				matchedLineslocal.add(line);
		}
		
		updateMatchedLines(matchedLineslocal);
		
	}

	private synchronized void updateMatchedLines(ArrayList<String> matchedLineslocal) {
		this.matchedLines.addAll(matchedLineslocal);		
	}
	
	@Override
	protected void afterProcess() {
		try{

			BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputDirectory.toString(),false));
			
			for (String line : matchedLines)
				values_writer.write(line+"\n");
			
			values_writer.flush();
			values_writer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
