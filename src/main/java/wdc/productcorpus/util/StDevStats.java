package wdc.productcorpus.util;

import java.util.ArrayList;
import java.util.Arrays;

public class StDevStats {

	public double getMedian(double[] counts) {
		Arrays.sort(counts);
		double median;
		if (counts.length % 2 == 0)
		    median = ((double)counts[counts.length/2] + (double)counts[counts.length/2 - 1])/2;
		else
		    median = (double) counts[counts.length/2];
		
		return median;
	}
	
	public double getMean(double[] counts)
    {
        double sum = 0.0;
        for(double a : counts)
            sum += a;
        return sum/counts.length;
    }

    public double getVariance(double[] counts)
    {
        double mean = getMean(counts);
        double temp = 0;
        for(double a :counts)
            temp += (mean-a)*(mean-a);
        return temp/counts.length;
    }

    public double getStdDev(double[] counts)
    {
        return Math.sqrt(getVariance(counts));
    }
    
    public double getVC(double[] counts) {
    	return getStdDev(counts) / getMean(counts);
    }
    
    public double[] toPrimDouble(ArrayList<Double> counts) {
    	double[] primitive = new double[counts.size()];
    	for(int i=0; i<counts.size(); i++) {
    		primitive[i] = counts.get(i);
    	}
    	return primitive;
    }
    
}
