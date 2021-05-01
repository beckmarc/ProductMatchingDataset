package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.util.FilterLinesOfFile;

public class HTMLTextualContentExtractor extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	
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
		String line="";
	
		ArrayList<String> htmlText = new ArrayList<String>();

		while ((line = br.readLine()) != null) {
			
			try {
				
				JSONObject json = new JSONObject(line);
				String url = json.getString("url");
				String content = json.getString("content");

				Document doc = Jsoup.parse(content);
				
 				String stripedcontent = doc.text();
 				Document doc_repeat = Jsoup.parse(stripedcontent);
 				
 				stripedcontent = doc_repeat.text();

				
				JSONObject strippedjson = new JSONObject();
				strippedjson.put("url", url);
				strippedjson.put("content", stripedcontent);
			
				htmlText.add(strippedjson.toString()+"\n");


			}
			catch (Exception e){
				System.out.println(e.getStackTrace());
				System.out.println(e.getMessage());
				continue;
			}
			
		}
		FileOutputStream output = new FileOutputStream(outputDirectory+"/"+object.getName().replace(".gz","")+"_text.gz");
		Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
		
		for (String item: htmlText)
			writer.write(item);

		writer.close();
		output.close();
	
	}

	
	@Override
	protected void afterProcess() {
		
			
		System.out.println("DONE");
		
	}
	
	
	public static void main (String args[]) throws IOException{
		
		HTMLTextualContentExtractor extracthtmltext = new HTMLTextualContentExtractor();
		new JCommander(extracthtmltext, args);
		extracthtmltext.process();
	}
}
