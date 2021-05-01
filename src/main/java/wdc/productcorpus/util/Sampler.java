package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;


/**
 * @author Anna Primpeli
 * takes as input a sorted by class label file and samples from the data to create a balanced set
 */
public class Sampler {

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-classSize", required = true, description = "Number of instances per label.")
	private Integer classSize;
	
	@Parameter(names = "-allowUpsampling", required = false, description = "Should we allow upsampling.")
	private Boolean allowUpsampling = false;
	
	@Parameter(names = "-labelPosition", required = true, description = "Position of the label element in the file (column number starting from 0).")
	private Integer labelPosition;
	
	@Parameter(names = "-separator", required = false, description = "Separator of the input file.")
	private String separator = "\t";
	
	private Integer downsampledRecords = 0;
	private Integer upsampledRecords = 0;
	
	public void process() throws Exception {
		
		BufferedReader br = InputUtil.getBufferedReader(inputDirectory);
		
		String line="";
		
		ArrayList<String> linesToWrite = new ArrayList<String>();
		
		ArrayList<String> recordsOfSameClass = new ArrayList<String>();
		String classLabel = "";
		
		while (br.ready()) {
			try {
				line = br.readLine();
				String currentClassLabel = line.split(separator)[labelPosition];
				
				if (classLabel.equals("")) classLabel = currentClassLabel;
				
				if (classLabel.equals(currentClassLabel)) recordsOfSameClass.add(line);
				else {
					linesToWrite.addAll(sample(recordsOfSameClass));
					recordsOfSameClass.clear();
					classLabel = currentClassLabel;
					recordsOfSameClass.add(line);
				}
			}
			catch(Exception e) {
				System.out.println(e.getMessage());
			}
		}

		write(linesToWrite);
		System.out.println("Generated count records: "+linesToWrite.size());
		System.out.println("In total downsampled for "+downsampledRecords+" class labels");
		System.out.println("In total upsampled for "+upsampledRecords+" class labels");
		System.out.println("DONE");
	}

	private void write( ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+inputDirectory.getName()+"_BALANCED_SAMPLE.txt",false));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	private ArrayList<String> sample(ArrayList<String> recordsOfSameClass) {

		int size = recordsOfSameClass.size();
		//downsampling
		if (size>classSize){
			downsampledRecords++;
			while (recordsOfSameClass.size()!=classSize) {
				int randomIndex = (int) ((Math.random() * recordsOfSameClass.size()));
				recordsOfSameClass.remove(randomIndex);
			}
		}
		
		//upsampling
		if (size<classSize && allowUpsampling){
			upsampledRecords++;
			while (recordsOfSameClass.size()!=classSize) {
				int randomIndex = (int) ((Math.random() * recordsOfSameClass.size()));
				recordsOfSameClass.add(recordsOfSameClass.get(randomIndex));
			}
		}
		return recordsOfSameClass;
	}
}
