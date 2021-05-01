package wdc.productcorpus.datacreator.Filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Eliminates the lines of the gold standard which include non important labels.
 * Non important labels are considered the ones that have a length smaller than 8 or bigger than 25 and the ones that do not include any digit
 */
public class ClassFilter extends Processor<File>{
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	long eliminatedLines = (long)0.0;
	
	private int minimumNumberofClassTokens = 8;
	private int maximumNumberofClassTokens = 25;
	
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
		Integer tmpeliminatedLines = 0;
		
		ArrayList<String> filteredData = new ArrayList<String>();
		String line;

		while ((line = br.readLine()) != null) {
			String classLabel =line.split("\\t")[4];
			
			if (isInterestingClass(classLabel)) filteredData.add(line);
			else tmpeliminatedLines++;
			
			if (filteredData.size()>100000) {
				writeInFile(object.getName(), filteredData);
				filteredData.clear();
			}
		}
		//write the last part
		writeInFile(object.getName(), filteredData);
		filteredData.clear();
		
		integrateEliminatedLines(tmpeliminatedLines);
	}
	
	private synchronized void integrateEliminatedLines (Integer lines) {
		this.eliminatedLines += lines;
	}

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_filteredClassLabels.txt",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	private boolean isInterestingClass(String classLabel) {
		if (!classLabel.matches(".*\\d+.*") ||classLabel.length()<minimumNumberofClassTokens || classLabel.length()>maximumNumberofClassTokens )
			return false;
		else return true;
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
