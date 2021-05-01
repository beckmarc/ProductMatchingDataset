package wdc.productcorpus.datacreator.Extractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import wdc.productcorpus.util.InputUtil;

/**
 * @author Anna Primpeli
 * Check if the input file is sorted by subjectID
 */
public class OrderVerifier extends Processor<File> {
 	
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
	
	HashSet<String> parsedNodesURLs = new HashSet<String>();
	boolean allSorted = true;
	long unsortedElements = (long) 0.0;
	
	@Override
	protected void process(File object) throws Exception {

		long unorderedInFile = isOrderedBySubjectID_URL(object.getPath());
		
		if (unorderedInFile>0.0) {
			System.out.println("Found unordered elements in file:"+object.getName());
			System.out.println("The combination of node ID and URL should be used as unique identifiers. Please check.");
			allSorted = false;
			unsortedElements+= unorderedInFile;
		}
		
	}
	
	@Override
	protected void afterProcess() {
		try {
			if (allSorted)
				System.out.println("All sorted");
			else
				System.out.println("Unsorted elements found: "+unsortedElements+" \n From a total of "+parsedNodesURLs+" unique nodes-urls.");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
	public long isOrderedBySubjectID_URL(String filePath) throws IOException {
		
		long unorderedElements = (long) 0.0;
		
		BufferedReader readerProductFile = InputUtil.getBufferedReader(new File(filePath));
		
		
		String currentSubjectURL = "";
		String line;
				
		while ((line = readerProductFile.readLine()) != null) {
			
			String [] lineparts = line.split("\\t");
			String id = lineparts[1]+lineparts[2];
			
			if (!id.equals(currentSubjectURL))	{	
				
				if (parsedNodesURLs.contains(id)){
					System.out.println("Found unordered item with id: "+lineparts[1] +" and url:"+lineparts[2]);
					unorderedElements++;
				}
				
				
				if (!currentSubjectURL.equals(""))
					parsedNodesURLs.add(currentSubjectURL);
				currentSubjectURL = id;
			}
		}
		return unorderedElements;
	}
	
	public boolean isOrderedBySubjectID(String filePath) throws IOException {
		
		System.out.println("First check if the input file is ordered by subject ID : "+filePath);

		BufferedReader readerProductFile = InputUtil.getBufferedReader(new File(filePath));
		
		QuadFileLoader qfl = new QuadFileLoader();
		
		String currentSubject = "";
		String line;
		
		Quad q = null;
		HashSet<String> parsedNodes = new HashSet<String>();
		boolean ordered = true;
		
		while ((line = readerProductFile.readLine()) != null) {
			
			q = qfl.parseQuadLine(line);
			if (!q.subject().toString().equals(currentSubject))	{	
				if (parsedNodes.contains(q.subject().toString())){
					System.out.println("Found unordered item with id: "+q.subject().toString());
					return false;
				}
				
				
				if (!currentSubject.equals(""))
					parsedNodes.add(currentSubject);
				currentSubject = q.subject().toString();
			}
		}
		return ordered;
	}
	
	public boolean isOrderedByURL(String filePath) throws IOException {
		
		System.out.println("First check if the input file is ordered by subject URL : "+filePath);

		BufferedReader readerProductFile = InputUtil.getBufferedReader(new File(filePath));
		
		QuadFileLoader qfl = new QuadFileLoader();
		
		String currentURL = "";
		String line;
		
		Quad q = null;
		HashSet<String> parsedNodes = new HashSet<String>();
		boolean ordered = true;
		
		while ((line = readerProductFile.readLine()) != null) {
			
			q = qfl.parseQuadLine(line);
			if (!q.graph().toString().equals(currentURL))	{	
				if (parsedNodes.contains(q.graph().toString())){
					System.out.println("Found unordered ited with id: "+q.subject().toString());
					return false;
				}
				
				
				if (!(q.graph().toString().equals("")))
					parsedNodes.add(q.graph().toString());
				currentURL = q.graph().toString();
			}
		}
		return ordered;
	}
}
