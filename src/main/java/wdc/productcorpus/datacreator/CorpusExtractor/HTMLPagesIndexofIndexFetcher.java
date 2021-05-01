package wdc.productcorpus.datacreator.CorpusExtractor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.archive.url.SURT;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;

public class HTMLPagesIndexofIndexFetcher extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-index",
	"-indexFile" }, required = true, description = "Main index file of plds - file directories", converter = FileConverter.class)
	private File index;

	@Parameter(names = { "-urlPos",
	"-urlPosition" }, required = true, description = "Position of url in the input files")
	private Integer urlPos;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private HashMap<String, HashSet<String>> filesOfDomains = new HashMap<String, HashSet<String>>();
	private HashSet<String> plds = new HashSet<String>();
	private HashSet<String> plds_fetched = new HashSet<String>();
	
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
		
		HashSet<String> urlsOfFile = getURLsofFile(object);

		HashSet<String> pldKeys = listOfPLDs(urlsOfFile);
		
		HashSet<String> plds_found = new HashSet<String>();
		
		//sort pldKeys alphabetically 
		ArrayList<String> pldKeysSorted = new ArrayList<String>(pldKeys);
		Collections.sort(pldKeysSorted);
		Iterator<String> pldKeyIterator = pldKeysSorted.iterator();

		//get a list of all plds
		integrateplds(pldKeys);
			
		BufferedReader br = InputUtil.getBufferedReader(index);
		
		HashMap<String, HashSet<String>> plds_file = new HashMap<String, HashSet<String>>();
		
		String currentline = br.readLine(); //for retrieving the file
		String lookaheadline; //for the comparison
		
		String pldKeyToSearch = "";

		while ((lookaheadline = br.readLine()) != null && pldKeyToSearch != null) {
			
			String lineKey = lookaheadline.split("\\)")[0];
	
			if (pldKeyToSearch.equals(""))
				pldKeyToSearch = pldKeyIterator.next();
			
			while (lineKey.compareTo(pldKeyToSearch) >= 0) {
				HashSet<String> existingFiles = plds_file.get(pldKeyToSearch);
				if (null == existingFiles) existingFiles = new HashSet<String>();
				existingFiles.add(currentline.split("\\t")[1]);
				plds_file.put(pldKeyToSearch, existingFiles);
				plds_found.add(pldKeyToSearch);
				if (pldKeyIterator.hasNext())
					pldKeyToSearch = pldKeyIterator.next();
				else {
					pldKeyToSearch = null;
					break;
				}
			}
			
			
			currentline = lookaheadline;
		}
		
		//fetch the last stuff - case where the remaining pld keys are bigger than the last entry of the index of the indices
		while (br.readLine() == null && pldKeyToSearch != null) {
			
			HashSet<String> existingFiles = plds_file.get(pldKeyToSearch);
			if (null == existingFiles) existingFiles = new HashSet<String>();
			existingFiles.add(currentline.split("\\t")[1]);
			plds_file.put(pldKeyToSearch, existingFiles);
			plds_found.add(pldKeyToSearch);			
			
			if (pldKeyIterator.hasNext())
				pldKeyToSearch = pldKeyIterator.next();
			else break;
		}
		
		integrateFilesOfDomains(plds_file);
		integratePLDsFetched(plds_found);
		
		br.close();
	}
	
	private synchronized void integratePLDsFetched(HashSet<String> plds_fetched) {
		this.plds_fetched.addAll(plds_fetched);
		
	}

	private HashSet<String> getURLsofFile(File object) throws IOException {
		
		HashSet<String> urlsOfFile = new HashSet<String>();
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		String line;

		while ((line = br.readLine()) != null) {
			String url = line.split("\\t")[urlPos];
			urlsOfFile.add(url);
		}
		
		br.close();
		
		return urlsOfFile;
	}
	
	private synchronized void integrateplds(HashSet<String> pldKeys) {
		this.plds.addAll(pldKeys);
		
	}

	private synchronized void integrateFilesOfDomains(HashMap<String, HashSet<String>> filesOfDomains) {
		
		this.filesOfDomains.putAll(filesOfDomains);
	}

	
	
	public HashSet<String> listOfPLDs(HashSet<String> urls) {
		
		try {
			HashSet<String> pldKeys = new HashSet<String>();
			
			for (String url: urls) {
				String surt = SURT.toSURT(url);
				if (null == surt) {
					System.out.println("Could not retrieve surt for url :"+url);
					continue;
				}

				String key = surt.substring(surt.indexOf("(")+1);
				if (key.contains(")")) key = key.substring(0, key.indexOf(")"));
				if (key.endsWith(",")) key = key.substring(0, key.length()-1);

				pldKeys.add(key);

			}
			
			return pldKeys;
		}
		catch (Exception e){
			System.out.println(e.getMessage());
			System.exit(0);
			return null;
		}
	}

	@Override
	protected void afterProcess() {
		try{
			
			System.out.println("Extracted SURT for "+plds.size()+" plds.");
			
			System.out.println("Retrieved file information for "+plds_fetched.size()+" plds.");
			
			System.out.println("No information for "+(plds.size()-plds_fetched.size())+" plds.");
			
			HashSet<String> notFoundPLDs = plds;
			notFoundPLDs.removeAll(plds_fetched);
			if (notFoundPLDs.size()>0){
				Iterator<String> iter = notFoundPLDs.iterator();
				System.out.println("Examples of not found plds :"+iter.next().toString()+"---- "+iter.next().toString());
			}

			// write plds - index files information 
			System.out.println("Write plds - index files information");
			BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/plds_filesinfo.txt",false));
			for (Map.Entry<String, HashSet<String>> i: filesOfDomains.entrySet()){
				if (i.getValue().size()>1) System.out.println(i.getKey()+" in more than one files");
				values_writer.write(i.getKey()+"\t"+i.getValue().iterator().next()+"\n");
			}
			
			values_writer.flush();
			values_writer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
