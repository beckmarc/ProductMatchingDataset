package wdc.productcorpus.v2.profiler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;
import de.dwslab.dwslib.util.io.InputUtil;
import wdc.productcorpus.util.DomainUtil;
import wdc.productcorpus.util.Histogram;
import wdc.productcorpus.util.SortMap;
import wdc.productcorpus.v2.model.Cluster;
import wdc.productcorpus.v2.model.ClusterStatic;
import wdc.productcorpus.v2.model.Entity;
import wdc.productcorpus.v2.model.EntityStatic;
import wdc.productcorpus.v2.util.CustomFileWriter;
import wdc.productcorpus.v2.util.PrintUtils;

/**
 * @author Marc Becker
 * 
 */
public class HistogramProfiler {


	@Parameter(names = { "-out",
	"-outputDir" }, required = true, description = "Folder where the outputfile(s) are written to.", converter = FileConverter.class)
	private File outputDirectory; 
	
	@Parameter(names = { "-in",
		"-inputDir" }, required = true, description = "Folder where the input is read from.", converter = FileConverter.class)
	private File inputDirectory;	
	
	@Parameter(names = "-file",  required = true, description = "Counts number of plds")
	private String file;
	 
	public void process() {
		offerPerCluster();
	}
	
	ArrayList<Interval> intervals = new ArrayList<Interval>(){{
		add(new Interval(80,89));
		add(new Interval(90,99));
		add(new Interval(100,109));
		add(new Interval(110,119));
		add(new Interval(120,129));
		add(new Interval(130,139));
		add(new Interval(140,149));
		add(new Interval(150,159));
		add(new Interval(160,169));
		add(new Interval(170,179));
		add(new Interval(180,189));
		add(new Interval(190,199));
		add(new Interval(200,209));
		add(new Interval(210,219));
		add(new Interval(220,229));
		add(new Interval(230));
//		add(new Interval(1,1));
//		add(new Interval(2,3));
//		add(new Interval(4,5));
//		add(new Interval(6,7));
//		add(new Interval(8,9));
//		add(new Interval(10,11));
//		add(new Interval(12,13));
//		add(new Interval(14,15));
//		add(new Interval(16,17));
//		add(new Interval(18,19));
//		add(new Interval(20,21));
//		add(new Interval(22));
		
//		add(new Interval(1,1));
//		add(new Interval(2,6));
//		add(new Interval(7,11));
//		add(new Interval(12,16));
//		add(new Interval(17,21));
//		add(new Interval(22,26));
//		add(new Interval(27,31));
//		add(new Interval(32,36));
//		add(new Interval(37,41));
//		add(new Interval(42,46));
//		add(new Interval(47,51));
//		add(new Interval(52));
		
//		add(new Interval(1,1));
//		add(new Interval(2,2));
//		add(new Interval(3,3));
//		add(new Interval(4,4));
//		add(new Interval(5,5));
//		add(new Interval(6));

	}};
	
	private void offerPerCluster() {
		String coord = "";
		String line;
		try {
			for(File f : inputDirectory.listFiles()) {
				if (!f.isDirectory() && f.getName().equals(file)) { 
					BufferedReader br = InputUtil.getBufferedReader(f);
					int entityCount = 0;
					while ((line = br.readLine()) != null) {
						String[] array = line.substring(1,line.length()).split("\\s:\\s");
						if(array.length == 2) { 
							int clusterSize = Integer.parseInt(array[0]);
							int amount = Integer.parseInt(array[1]);
							for(Interval i : intervals) {
								if(i.isInInterval(clusterSize)) {
									i.offerPair.addValues(amount, calcPairs(clusterSize, amount));
								}
							}
							//coord = "(" + clusterSize + "," + amount + ")" + coord;
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintUtils.pal(intervals);
		PrintUtils.p(getPlotString(intervals));
		//PrintUtils.p(coord);
	}
	
	/**
	 * Gets the string to plot the histogram aggregation in latex with tikz
	 * @param intervals
	 */
	private String getPlotString(ArrayList<Interval> intervals) {
		String result = "";
		for(Interval i : intervals) {
			result += "(" + i.offerPair.offers + "," + i.getIntervalString() + ") ";
		}
		result += "\n";
		for(Interval i : intervals) {
			result += i.getIntervalString() + ",";
		}
		return result;
	}
	

	private int calcPairs(int clusterSize, int offers) {
		return ((clusterSize * (clusterSize - 1))/2)*offers;
	}
	
	public static void main(String[] args) {
		HistogramProfiler hp = new HistogramProfiler();
		try {
			new JCommander(hp, args);
			hp.process();
		} catch (ParameterException pe) {
			pe.printStackTrace();
			new JCommander(hp).usage();
		}
	}
	
}



class OfferPair {
	public int offers;
	public int pairs;
	
	public OfferPair(int offers, int pairs) {
		this.offers = offers;
		this.pairs = pairs;
	}
	
	public void addValues(int offers, int pairs) {
		this.offers += offers;
		this.pairs += pairs;
	}
}

class Interval {
	public int start;
	public int end;
	OfferPair offerPair;
	
	public Interval(int start) {
		this.start = start;
		this.offerPair = new OfferPair(0,0);
	}
	
	public Interval(int start, int end) {
		this.start = start;
		this.end = end;
		this.offerPair = new OfferPair(0,0);
	}
	
	public boolean isInInterval(int value) {
		if(end == 0) {
			return start <= value;
		}
		return start <= value && value <= end;
	}
	
	public String getIntervalString() {
		if(start == end) {
			return  "[" + start + "]";
		}
		if(end == 0) {
			return  "[>" + start + "]";
		}
		return "[" + start + "-" + end + "]";
	}
	
	public String getFormattedOffersAmount() {
		return String.format(Locale.US, "%,d", offerPair.offers);
	}
	
	public String getFormattedOfferPairsAmount() {
		return String.format(Locale.US, "%,d", offerPair.pairs);
	}
	
	@Override
	public String toString() {
		return getIntervalString() + " "+ getFormattedOffersAmount() + " "+ getFormattedOfferPairsAmount();
	}
 }
