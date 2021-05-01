package wdc.productcorpus.datacreator.OutputFilesCreator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import com.google.common.net.InternetDomainName;

import de.dwslab.dwslib.framework.Processor;
import de.wbsg.loddesc.util.DomainUtils;
import wdc.productcorpus.util.InputUtil;

public class EnglishCorpusFilter extends Processor<File> {
	
	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;

	long englishOffers = (long)0.0;
	ArrayList<String> engtlds = new ArrayList<String>(){{
	    add("com");
	    add("net");
	    add("co.uk");
	    add("us");
	    add("org");
	}};

	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		if (inputDirectory.isDirectory()) {
			for (File f : inputDirectory.listFiles()) {
				if (!f.isDirectory()) {
					files.add(f);
				}
			}
		}
		else files.add(inputDirectory);
		
		return files;
	}
	
	@Override
	protected void process(File object) throws Exception {
		
		BufferedReader br = InputUtil.getBufferedReader(object);
		
		ArrayList<String> englishOffersLocal = new ArrayList<String>();
		long counter = (long)0.0;
		String line;
		String domain = null;
		InternetDomainName internetDomain = null;
		String tld =null;

		while ((line = br.readLine()) != null) {
			JSONObject json = new JSONObject(line);
			String url = json.getString("url");
			try {				
				domain = DomainUtils.getDomain(url);
				internetDomain = InternetDomainName.from(domain);
				if (null != internetDomain.publicSuffix())
					tld = internetDomain.publicSuffix().name().toString();
				else continue;
				
				if (null != tld && engtlds.contains(tld) ) {
						englishOffersLocal.add(line);
						counter++;
				}
			}
			catch (Exception e) {
				System.out.println(e.getMessage());
				continue;
			}
			
			if (englishOffersLocal.size() == 10000) {
				writeInFile(object.getName(), englishOffersLocal);
				englishOffersLocal.clear();
			}
			
		}
		//write the last part
		writeInFile(object.getName(), englishOffersLocal);
		englishOffersLocal.clear();
		
		integrateCounter(counter);
		
		br.close();
	}
	
	private synchronized void integrateCounter (long lines) {
		this.englishOffers += lines;
	}

	private synchronized void writeInFile(String fileName, ArrayList<String> data) throws IOException {
		
		BufferedWriter writer = new BufferedWriter (new FileWriter(outputDirectory.toString()+"/"+fileName+"_english.json",true));
		
		for (String line:data)
			writer.write(line+"\n");
		
		writer.flush();
		writer.close();
		
	}
	
	@Override
	protected void afterProcess() {
		try {

			System.out.println("English offers: "+this.englishOffers);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
