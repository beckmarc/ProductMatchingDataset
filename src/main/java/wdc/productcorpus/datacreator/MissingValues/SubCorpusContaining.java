package wdc.productcorpus.datacreator.MissingValues;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Filter the product corpus to get info about certain nodes given in the patterns file
 */
public class SubCorpusContaining extends Processor<File> {

	@Parameter(names = { "-patterns",
	"-patternsFile" }, required = true, description = "File that contains the patterns which should be searched.", converter = FileConverter.class)
	private File patternsFile;	
	
	@Parameter(names = { "-in",
	"-inputDir" }, required = true, description = "Folder where the input corpus is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the output is written to.", converter = FileConverter.class)
	private File outputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private ArrayList<String> matchedLines = new ArrayList<String>();
	private HashMap<String, String> patterns;

	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		
		if (inputDirectory.isFile()) 
			files.add(inputDirectory);
		else {
			for (File f : inputDirectory.listFiles()) {
				if (!f.isDirectory()) {
					files.add(f);
				}
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
		QuadFileLoader qfl = new QuadFileLoader();


		while (br.ready()) {
			
			line = br.readLine();
			Quad q = qfl.parseQuadLine(line);
			
			if (patterns.containsKey(q.subject().toString())){
				if  (patterns.get(q.subject().toString()).equals(q.graph()))
					matchedLineslocal.add(line);
			}
			else if (patterns.containsKey(q.value().toString())){
				if  (patterns.get(q.value().toString()).equals(q.graph()))
					matchedLineslocal.add(line);
			}
				
		}
		
		updateMatchedLines(matchedLineslocal);
		
	}

	private synchronized void updateMatchedLines(ArrayList<String> matchedLineslocal) {
		this.matchedLines.addAll(matchedLineslocal);		
	}
	
	@Override
	protected void afterProcess() {
		try{

			System.out.println("Write found patterns");
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
	
	@Override
	protected void beforeProcess() {
		
		try{
			patterns = new HashMap<String, String>();
			
			BufferedReader br = new BufferedReader(new FileReader((patternsFile)));

			String line;		
			
			
			QuadFileLoader qfl = new QuadFileLoader();


			while (br.ready()) {
				
				line = br.readLine();
				Quad q = qfl.parseQuadLine(line);
				
				
//				JSONObject json = new JSONObject(line);
//				
//				String url = json.getString("url");
//				String nodeID = json.getString("nodeID");
				
				patterns.put(q.subject().toString(), q.graph());
			}
			
			br.close();
			
			System.out.println("Number of patterns loaded: "+patterns.size());
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
	}
}
