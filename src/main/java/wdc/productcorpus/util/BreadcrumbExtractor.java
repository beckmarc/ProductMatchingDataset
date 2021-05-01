package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import ldif.local.datasources.dump.QuadFileLoader;
import ldif.runtime.Quad;
import wdc.productcorpus.datacreator.Master;

public class BreadcrumbExtractor extends Processor<File> {

	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;
	
	@Parameter(names = { "-urls",
	"-urlsFile" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File urlList;
	
	@Parameter(names = "-threads", required = true, description = "Number of threads.")
	private Integer threads;
	
	private HashSet<String> urls = new HashSet<String>();
	private HashSet<String> allBreadCrumbInfo = new HashSet<String>();

	public static void main(String args[]) {
		BreadcrumbExtractor extractBC = new BreadcrumbExtractor();
		JCommander jc = new JCommander(extractBC);
		jc.parse(args);
		extractBC.process();
	}
	@Override
	protected void beforeProcess() {
		try {
			BufferedReader br = InputUtil.getBufferedReader(this.urlList);
			String line;
			while ((line = br.readLine()) != null) {
				urls.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	
	public void process (File object) throws IOException {
		
		
		HashSet<String> breadbrumbInfo = new HashSet<String>();
		String line;
		
		QuadFileLoader qfl = new QuadFileLoader();
		// read the file
		BufferedReader br = InputUtil.getBufferedReader(object);
		while ((line=br.readLine())!=null) {
			try {
				Quad q = qfl.parseQuadLine(line);
				String url = q.graph().toString();
				if (urls.contains(url)  && (line.toLowerCase().contains("breadcrumb") || line.toLowerCase().contains("itemlistelement")|| line.toLowerCase().contains("listitem")))
					breadbrumbInfo.add(line);
			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
		}
		
		integrateBreadcrumbInfo(breadbrumbInfo);
	}
	
	private synchronized void integrateBreadcrumbInfo(HashSet<String> info) throws IOException {
		
		this.allBreadCrumbInfo.addAll(info);
		if (this.allBreadCrumbInfo.size()>=10000) {
			BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString(),true));
			
			for (String line:this.allBreadCrumbInfo)
				writer.write(line+"\n");
			
			writer.flush();
			writer.close();
			this.allBreadCrumbInfo.clear();

		}
	}

	@Override
	protected void afterProcess() {
		try {
			BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString(),true));
			
			for (String line:this.allBreadCrumbInfo)
				writer.write(line+"\n");
			
			writer.flush();
			writer.close();
			this.allBreadCrumbInfo.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
