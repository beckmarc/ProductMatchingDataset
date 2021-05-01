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
 * Eliminates the lines of the gold standard that included non important textual values.
 * Non important textual values are considered the ones that have less than x (defined by user in the input params - default 5) words.
 */
public class TextualValueFilter extends Processor<File>{
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-minWords", required = false, description = "Number of threads.")
	private int minWords = 5;
	
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

		while ((line = br.readLine()) != null) {
			String textualValue =line.split("\\t")[5];
			
			if (isInterestingTextualValue(textualValue)) filteredData.add(line);
			else eliminatedLines++;
			
			if (filteredData.size()>100000) {
				writeInFile(object.getName(), filteredData);
				filteredData.clear();
			}
		}
		//write the last part
		writeInFile(object.getName(), filteredData);
		filteredData.clear();
	}

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_filteredTextualValues.txt",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	/**
	 * @param textualValue
	 * @return
	 * Eliminate the textual values (documents) that have less than a defined number of words
	 */
	private boolean isInterestingTextualValue(String textualValue) {
		
		String words [] = textualValue.replaceAll("\"","").trim().split(" ");
		if (words.length<minWords)
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
