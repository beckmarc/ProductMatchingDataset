package wdc.productcorpus.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import scala.reflect.generic.Trees.This;
import wdc.productcorpus.v2.util.PrintUtils;

public class Histogram<T> {

	LinkedHashMap <String, Integer> distr = new LinkedHashMap <String, Integer>();
	String hTitle;
	boolean save = false;
	FileWriter output;
	
	public Histogram (String title, boolean save, FileWriter fileWriter) {
		this.hTitle = title;
		this.save = save;
		this.output = fileWriter;
	}
	
	
	public LinkedHashMap <String, Integer> drawDistr(Integer binSize, HashMap<T, Integer> data){
		
		Integer maxValue = findMaximumValue(data);
		distr = initializeMap(maxValue, binSize);
		try{
			
			for (Map.Entry<T, Integer> d:data.entrySet()) {

                for (int i =maxValue; i>=0; i=i-binSize){
        			if (d.getValue()>i) {
        				int temp = distr.get(">"+i);
        				distr.put(">"+i, temp+1);
        				break;
        			}
        		}
			}
			
			if (save) saveHistogram();
			return distr;
		}
		catch (Exception e){
			e.printStackTrace();
			System.out.println(e.getMessage());
			return null;
		}
	}

	private Integer findMaximumValue(HashMap<T, Integer> data) {
		
		Integer max = Collections.max(data.values());
		return max;
		
	}

	private LinkedHashMap<String, Integer> initializeMap(Integer maxValue, Integer binSize) {

		LinkedHashMap<String, Integer> distr = new LinkedHashMap<String, Integer>();
		
		for (int i =maxValue; i>=0; i=i-binSize){
			distr.put(">"+i, 0);
		}
		return distr;
	}
	
	private void saveHistogram() throws IOException {
		
		BufferedWriter writer  = new BufferedWriter(output);
		
		writer.write("Histogram "+this.hTitle+"\n" );
		
		//PrintUtils.p(this.distr);
		
		String key = "";
		Integer value = 0;
		boolean first = true;
		for (Map.Entry<String, Integer> h:this.distr.entrySet()) {		
			if(first) {
				key = h.getKey();
				first = false;
			} else {
				value = h.getValue();
				writer.write(key+" : "+value+"\n");
				key = h.getKey();
			}
			
		}
		
		writer.flush();
		writer.close();
	}


	

	
}
