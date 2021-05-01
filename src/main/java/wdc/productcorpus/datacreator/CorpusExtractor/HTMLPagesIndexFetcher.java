package wdc.productcorpus.datacreator.CorpusExtractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.archive.util.SURT;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;

public class HTMLPagesIndexFetcher extends Processor<File>{

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-indexFiles",
	"-indexFilesDir" }, required = true, description = "Folder where the index files are found", converter = FileConverter.class)
	private File indexDir;
	
	@Parameter(names = { "-pldIndexMap",
	"-pldIndexMapDir" }, required = true, description = "File that maps SURTs with the index file", converter = FileConverter.class)
	private File pldIndexMap;
	
	@Parameter(names = { "-urlPos",
	"-urlPosition" }, required = true, description = "Which part of the tab separated file the url is located")
	private Integer urlPos;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	HashMap<String,String> pldIndex = new HashMap<String,String>();
	HashSet<String> indexOfPagesInfo = new HashSet<String>();
	HashSet<String> allPages = new HashSet<String>();
	
	HashMap<String, HashSet<String>> urlsToFiles = new HashMap<String,HashSet<String>>();
	
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
	
	/**
	 * Read the pldIndex file before start processing
	 */
	@Override
	protected void beforeProcess() {
		
		try{
			
			BufferedReader reader = new BufferedReader(new FileReader((pldIndexMap)));

			String line;		

			while ((line = reader.readLine()) != null) {
				pldIndex.put(line.split("\t")[0], line.split("\t")[1]);
			}
			
			reader.close();
			
			System.out.println("Number of plds (as surt) index files loaded: "+pldIndex.size());
		}
		catch (Exception e){
			System.out.println(e.getMessage());
		}
		
	}
	
	
	/* (non-Javadoc)
	 * not very optimal but will do - change
	 * get the url of every entry - extract SURT -locate the index file and save the index info 
	 */
	@Override 
	protected void process(File object) throws Exception {
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		HashSet<String> allPages_ = new HashSet<String>();
		HashMap<String,HashSet<String>> localUrlsToFiles = new HashMap<String,HashSet<String>>();
		
		String line="";

		while (br.ready()) {
			line = br.readLine();
			String url = line.split("\t")[urlPos];
			allPages_ .add(url);
			String surt = SURT.fromURI(url);
			
			//surt normalization
			surt = surt.substring(surt.indexOf("(")+1);
			if (surt.contains(")")) surt = surt.substring(0, surt.indexOf(")"));
			if (surt.endsWith(",")) surt = surt.substring(0, surt.length()-1);
			
			String fileWithSurt = pldIndex.get(surt);
			if (null == fileWithSurt) {
				System.out.println("Could not find SURT "+surt+" in the pld index file");
			}
			else {
				
				HashSet<String> currentURLs = localUrlsToFiles.get(fileWithSurt);
				if (null == currentURLs) currentURLs = new HashSet<String>();
				currentURLs.add(url);
				localUrlsToFiles.put(fileWithSurt, currentURLs);
				 
			}
		}
		
		
		integrateUrlsfileInfo(localUrlsToFiles);
		
		br.close();
	}

	private synchronized void integrateUrlsfileInfo(HashMap<String, HashSet<String>> localUrlsToFiles) {
		for (String file : localUrlsToFiles.keySet()) {
			HashSet<String> urls = this.urlsToFiles.get(file);
			if (urls == null) {
				urls = localUrlsToFiles.get(file);
			} else {
				urls.addAll(localUrlsToFiles.get(file));
			}
			this.urlsToFiles.put(file, urls);
		}
		
	}



//	private void getIndexInfo() throws IOException{
//		
//		BufferedReader br = null;
//		int filesCount = urlsToFiles.size();
//		int done=0;
//		
//		try{
//			for (Map.Entry<String, HashSet<String>> searchFile: urlsToFiles.entrySet()) {
//				System.out.println("Searched in "+done+" out of "+filesCount+" index files.");
//				br = InputUtil.getBufferedReader(new File(indexDir+"/"+searchFile.getKey()));
//				String line="";
//
//				while ((line = br.readLine()) != null) {
//					
//					String urlOfLine = line.substring(line.indexOf("\"url\": \"")+8, line.indexOf("\", \"mime\":"));
//					if (searchFile.getValue().contains(urlOfLine)) indexOfPagesInfo.add(line);
//				}
//				done++;
//
//			}
//			br.close();
//		}
//		catch (Exception e){
//			System.out.println(e.getMessage());
//		}
//				
//	}
	
	@Override
	protected void afterProcess() {
		try{
			
			System.out.println("Get "
					+ " info");
			HTMLPagesIndexFetcherProcessor getIndex = new HTMLPagesIndexFetcherProcessor(outputDirectory, indexDir, threads, urlsToFiles);
			getIndex.process();
		
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
