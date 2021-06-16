package wdc.productcorpus.datacreator.Profiler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import wdc.productcorpus.util.InputUtil;
import wdc.productcorpus.util.SortMap;

/**
 * @author Anna Primpeli
 */
public class IDPropsProfiler extends Processor<File> {


	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	
	
	private HashMap<String, HashSet<String>> idprops_values = new HashMap<String,HashSet<String>>();

	public static void main (String args[]) {
		
		IDPropsProfiler profile = new IDPropsProfiler();
		new JCommander(profile, args);
		

		profile.process();
	}
	@Override
	protected void beforeProcess() {
	
		idprops_values.put("/gtin8", new HashSet<String>());
		idprops_values.put("/gtin12", new HashSet<String>());
		idprops_values.put("/gtin13", new HashSet<String>());
		idprops_values.put("/gtin14", new HashSet<String>());
		idprops_values.put("/sku", new HashSet<String>());
		idprops_values.put("/identifier", new HashSet<String>());
		idprops_values.put("/productID", new HashSet<String>());
		idprops_values.put("/mpn", new HashSet<String>());
		idprops_values.put("/gtin", new HashSet<String>());
		idprops_values.put("/serialNumber", new HashSet<String>());
	}
	
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
		
		HashMap<String, HashSet<String>> localMap = new HashMap<String,HashSet<String>>();
		localMap.put("/gtin8", new HashSet<String>());
		localMap.put("/gtin12", new HashSet<String>());
		localMap.put("/gtin13", new HashSet<String>());
		localMap.put("/gtin14", new HashSet<String>());
		localMap.put("/sku", new HashSet<String>());
		localMap.put("/identifier", new HashSet<String>());
		localMap.put("/productID", new HashSet<String>());
		localMap.put("/mpn", new HashSet<String>());
		localMap.put("/gtin", new HashSet<String>());
		localMap.put("/serialNumber", new HashSet<String>());
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		String line="";

		while (br.ready()) {
			try {
				line = br.readLine();
				String [] lineParts =line.split("\\t");
								
				//countValues
				String property  = lineParts[3];
				String value = lineParts[4];
				
				localMap.get(property).add(value);
				

			}
			catch (Exception e){
				System.out.println(e.getMessage());
				System.out.println("Line could not be parsed:"+line);
				continue;
			}
		}
		
		integrateValues(localMap);
		
		br.close();
		
	}


	private synchronized void integrateValues(HashMap<String, HashSet<String>> valueslocal) {
		for (String id_prop : valueslocal.keySet()) {
			
			this.idprops_values.get(id_prop).addAll(valueslocal.get(id_prop));
		}
		
	}

	@Override
	protected void afterProcess() {
		try{

			// write unique values count
			System.out.println("Write id properties unique values");
			for (Map.Entry<String, HashSet<String>> prop_values: this.idprops_values.entrySet()) {
				BufferedWriter values_writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+prop_values.getKey()+"_uniquevalues.txt",false));
				for (String value:prop_values.getValue())
					values_writer.write(value+"\n");

				values_writer.flush();
				values_writer.close();
			}

			//containment
			IDPropsOverlap overlap = new IDPropsOverlap();
			//sku
			
			if (this.idprops_values.get("/gtin8").size() < this.idprops_values.get("/sku").size() )
				System.out.println("SKUs in GTIN8s : "+overlap.containment(this.idprops_values.get("/sku"), this.idprops_values.get("/gtin8")));
			else 
				System.out.println("GTIN8s in SKUs : "+overlap.containment(this.idprops_values.get("/gtin8"), this.idprops_values.get("/sku")));
			
			if (this.idprops_values.get("/gtin13").size() < this.idprops_values.get("/sku").size() )
				System.out.println("SKUs in GTIN13s : "+overlap.containment(this.idprops_values.get("/sku"), this.idprops_values.get("/gtin13")));
			else 
				System.out.println("GTIN13s in SKUs : "+overlap.containment(this.idprops_values.get("/gtin13"), this.idprops_values.get("/sku")));
			
			if (this.idprops_values.get("/gtin12").size() < this.idprops_values.get("/sku").size() )
				System.out.println("SKUs in GTIN12s : "+overlap.containment(this.idprops_values.get("/sku"), this.idprops_values.get("/gtin12")));
			else 
				System.out.println("GTIN12s in SKUs : "+overlap.containment(this.idprops_values.get("/gtin12"), this.idprops_values.get("/sku")));
			
			if (this.idprops_values.get("/gtin14").size() < this.idprops_values.get("/sku").size() )
				System.out.println("SKUs in GTIN14s : "+overlap.containment(this.idprops_values.get("/sku"), this.idprops_values.get("/gtin14")));
			else 
				System.out.println("GTIN14s in SKUs : "+overlap.containment(this.idprops_values.get("/gtin14"), this.idprops_values.get("/sku")));
			
			//identifier
			if (this.idprops_values.get("/gtin8").size() < this.idprops_values.get("/identifier").size() )
				System.out.println("IDENTIFIERs in GTIN8s : "+overlap.containment(this.idprops_values.get("/identifier"), this.idprops_values.get("/gtin8")));
			else 
				System.out.println("GTIN8s in IDENTIFIERs : "+overlap.containment(this.idprops_values.get("/gtin8"), this.idprops_values.get("/identifier")));
			
			if (this.idprops_values.get("/gtin13").size() < this.idprops_values.get("/identifier").size() )
				System.out.println("IDENTIFIERs in GTIN13s : "+overlap.containment(this.idprops_values.get("/identifier"), this.idprops_values.get("/gtin13")));
			else 
				System.out.println("GTIN13s in IDENTIFIERs : "+overlap.containment(this.idprops_values.get("/gtin13"), this.idprops_values.get("/identifier")));
			
			if (this.idprops_values.get("/gtin12").size() < this.idprops_values.get("/identifier").size() )
				System.out.println("IDENTIFIERs in GTIN12s : "+overlap.containment(this.idprops_values.get("/identifier"), this.idprops_values.get("/gtin12")));
			else 
				System.out.println("GTIN12s in IDENTIFIERs : "+overlap.containment(this.idprops_values.get("/gtin12"), this.idprops_values.get("/identifier")));
			
			if (this.idprops_values.get("/gtin14").size() < this.idprops_values.get("/identifier").size() )
				System.out.println("IDENTIFIERs in GTIN14s : "+overlap.containment(this.idprops_values.get("/identifier"), this.idprops_values.get("/gtin14")));
			else 
				System.out.println("GTIN14s in IDENTIFIERs : "+overlap.containment(this.idprops_values.get("/gtin14"), this.idprops_values.get("/identifier")));
			
			
			//productID
			if (this.idprops_values.get("/gtin8").size() < this.idprops_values.get("/productID").size() )
				System.out.println("PRODUCTIDs in GTIN8s : "+overlap.containment(this.idprops_values.get("/productID"), this.idprops_values.get("/gtin8")));
			else 
				System.out.println("GTIN8s in PRODUCTIDs : "+overlap.containment(this.idprops_values.get("/gtin8"), this.idprops_values.get("/productID")));
			
			if (this.idprops_values.get("/gtin13").size() < this.idprops_values.get("/productID").size() )
				System.out.println("PRODUCTIDs in GTIN13s : "+overlap.containment(this.idprops_values.get("/productID"), this.idprops_values.get("/gtin13")));
			else 
				System.out.println("GTIN13s in PRODUCTIDs : "+overlap.containment(this.idprops_values.get("/gtin13"), this.idprops_values.get("/productID")));
			
			if (this.idprops_values.get("/gtin12").size() < this.idprops_values.get("/productID").size() )
				System.out.println("PRODUCTIDs in GTIN12s : "+overlap.containment(this.idprops_values.get("/productID"), this.idprops_values.get("/gtin12")));
			else 
				System.out.println("GTIN12s in PRODUCTIDs : "+overlap.containment(this.idprops_values.get("/gtin12"), this.idprops_values.get("/productID")));
			
			if (this.idprops_values.get("/gtin14").size() < this.idprops_values.get("/productID").size() )
				System.out.println("PRODUCTIDs in GTIN14s : "+overlap.containment(this.idprops_values.get("/productID"), this.idprops_values.get("/gtin14")));
			else 
				System.out.println("GTIN14s in PRODUCTIDs : "+overlap.containment(this.idprops_values.get("/gtin14"), this.idprops_values.get("/productID")));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
