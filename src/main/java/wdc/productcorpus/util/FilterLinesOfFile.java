package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.json.JSONObject;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;


public class FilterLinesOfFile extends Processor<File>{
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "path of outputfile", converter = FileConverter.class)
	private File outputFile; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	@Parameter(names = "-filter", required = false, description = "Number of threads.", converter = FileConverter.class)
	private File filterFile;
	
	long filteredLines = (long)0.0;
	
	HashSet<Integer> filterValues = new HashSet<Integer>();
	HashSet<String> linesToadd  = new HashSet<String>();
	Integer fileoutput = 1;
	
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
		
		Set<String> filteredData = new HashSet<String>();
		String line;

		while ((line = br.readLine()) != null) {
			try{
				JSONObject json = new JSONObject(line);
				Integer nodeid = json.getInt("id");
				//String url = json.getString("url");
				if(null==nodeid) continue;
				if (filterValues.contains(nodeid))
					filteredData.add(line);
				
				if (filteredData.size()%50000==0){
					System.out.println("Have loaded "+filteredData.size()+" filtered lines");
				}
			}
			
			catch(Exception e) {
				System.out.println(e.toString());
				continue;
			}			
		}
		integrateData(filteredData);
	}

	private synchronized void integrateData(Set<String> data) throws IOException {
		
		this.linesToadd.addAll(data);
		
		if (this.linesToadd.size()>=50000) {
			
			FileOutputStream output = new FileOutputStream(new File (outputFile.getName()+"_"+fileoutput));
			try {
			  Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
			  try {
				  for (String line:this.linesToadd)
						writer.write(line+"\n");
			  } finally {
			    writer.close();
			  }
			 } finally {
			   output.close();
			 }

			this.linesToadd.clear();

			this.fileoutput++;
		}
	}
	
	@Override
	protected void beforeProcess() {
		BufferedReader br;
		try {
			br = InputUtil.getBufferedReader(this.filterFile);
			String line;

			while ((line = br.readLine()) != null) {
				filterValues.add(Integer.valueOf(line));
			}
			
			System.out.println("Need to extract "+filterValues.size()+" lines");
			br.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	protected void afterProcess() {
		System.out.println("Will now write "+this.linesToadd.size()+" lines");
		try{
			FileOutputStream output = new FileOutputStream(new File (outputFile.getName()+"_"+fileoutput));
			try {
			  Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
			  try {
				  for (String line:this.linesToadd)
						writer.write(line+"\n");
			  } finally {
			    writer.close();
			  }
			 } finally {
			   output.close();
			 }	
		}		
		
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void main(String args[]) {
		FilterLinesOfFile filter = new FilterLinesOfFile();
		new JCommander(filter, args);
		filter.process();

	}
}
