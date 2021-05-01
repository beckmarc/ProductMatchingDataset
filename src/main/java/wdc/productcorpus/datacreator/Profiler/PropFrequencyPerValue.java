package wdc.productcorpus.datacreator.Profiler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.SortMap;

/**
 * @author Anna Primpeli
 *  Profiling of a single property in tsv/csv files. 
 */
public class PropFrequencyPerValue extends Processor<File> {


	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-propPos", required = true, description = "Position of property.")
	private Integer propPos;
	
	@Parameter(names = "-separator", required = true, description = "Separator of file.")
	private String separator;
	
	long parsedLines = (long)0.0;
	private HashMap<String, Integer> valuesCount = new HashMap<String,Integer>();

	
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
		
		long lineCount = (long)0.0;
		HashMap<String, Integer> valuesCount = new HashMap<String,Integer>();
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		String line="";

		while (br.ready()) {
			try {
				line = br.readLine();
				lineCount++;
				String [] lineParts =line.split(separator);
								
				//countValues
				String value = lineParts[propPos];
				Integer currentvaluecount = valuesCount.get(value);
				if (null == currentvaluecount) currentvaluecount=0;
				valuesCount.put(value, ++currentvaluecount);
				

			}
			catch (Exception e){
				System.out.println(e.getMessage());
				System.out.println("Line could not be parsed:"+line);
				continue;
			}
		}
		
		integrateValues(valuesCount);
		updateLineCount(lineCount);
		
		br.close();
		
	}

 

	private synchronized void updateLineCount(long lineCount) {
		this.parsedLines += lineCount;
		
	}


	
	private synchronized void integrateValues(HashMap<String, Integer> valuesCount) {
		for (String id : valuesCount.keySet()) {
			Integer value = this.valuesCount.get(id);
			if (value == null) {
				value = valuesCount.get(id);
			} else {
				value += valuesCount.get(id);
			}
			this.valuesCount.put(id, value);
		}
		
	}

	@Override
	protected void afterProcess() {
		try{

			// write unique values count
			System.out.println("Write stats about values count");
			BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/property_profiling.txt",false));
			LinkedHashMap<String, Integer> sorted_valuesCount = SortMap.sortByValue(valuesCount);

			
			for (Map.Entry<String, Integer> value : sorted_valuesCount.entrySet())
				values_writer.write(value.getKey()+"\t"+value.getValue()+"\n");
			
			values_writer.flush();
			values_writer.close();
	
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
