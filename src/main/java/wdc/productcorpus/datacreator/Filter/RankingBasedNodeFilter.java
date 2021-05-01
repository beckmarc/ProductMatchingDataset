package wdc.productcorpus.datacreator.Filter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;

/**
 * @author Anna Primpeli
 * Filters the nodes that have more than one identifiers. The filtering is based on a ranking scheme.
 * The identifying properties are ranked based on their schema.org/Product description and length
 * Give 0 scoring if you don't want to include an identifying property at all
 */
public class RankingBasedNodeFilter extends Processor<File>{

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private HashMap<String, Integer> propRanking = new HashMap<String, Integer> (){
		private static final long serialVersionUID = 1L;

		{
			put("/gtin14",    8);
			put("/gtin13",    7);
			put("/gtin12",    6);
			put("/gtin8" ,    5);
			put("/mpn"   ,    4);
			put("/identifier",3);
			put("/productID", 2);
			put("/sku"   ,    1);
		}
	};
	
	long eliminatedNodes = (long)0.0;

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
		String currentnodeid_url = "";
		String line;
		
		ArrayList<String> linesOfSameNode = new ArrayList<String>();
		long eliminated = (long) 0.0;

		while ((line = br.readLine()) != null) {
			String lineParts[] = line.split("\\t");
			String nodeid_url =lineParts[1].concat(lineParts[2]);

			
			if (currentnodeid_url.equals("")) currentnodeid_url=nodeid_url;
			
			if (nodeid_url.equals(currentnodeid_url)) {
				if (propRanking.get(lineParts[3]) != 0)
					linesOfSameNode.add(line);
				else eliminated++;
			}
			else {
				if (linesOfSameNode.size() == 1) filteredData.addAll(linesOfSameNode);
				else if (linesOfSameNode.size() > 1){
					eliminated += linesOfSameNode.size()-1;
					filteredData.add(selectLineBasedonIDProp(linesOfSameNode));
				}
				
				linesOfSameNode.clear();
				if (propRanking.get(lineParts[3]) != 0)
					linesOfSameNode.add(line);
				else eliminated++;
				currentnodeid_url = nodeid_url;
			}
			
			
			if (filteredData.size()>100000) {
				writeInFile(object.getName(), filteredData);
				filteredData.clear();
			}
		}
		//write the last part
		writeInFile(object.getName(), filteredData);
		filteredData.clear();
		updateEliminatedNodes(eliminated);
	}
	
	private synchronized void updateEliminatedNodes(long eliminated) {
		this.eliminatedNodes += eliminated;
		
	}

	private String selectLineBasedonIDProp(ArrayList<String> linesOfSameNode) {
		
		int maxScore =0;
		String bestLine ="";
		
		for (String l:linesOfSameNode) {
			String idProp = l.split("\\t")[3];
			int currentScore = propRanking.get(idProp);
			if (currentScore>maxScore) {
				maxScore = currentScore;
				bestLine = l;
			}
		}
		
		return bestLine;
	}

	private void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_filteredNodes.txt",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	@Override
	protected void afterProcess() {
		System.out.println("Eliminated Duplicate Nodes based on the ranking scheme of the identifying properties: "+this.eliminatedNodes);
	}
}
