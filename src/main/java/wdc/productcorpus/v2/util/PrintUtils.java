package wdc.productcorpus.v2.util;

import java.util.ArrayList;
import java.util.HashSet;

import scala.actors.threadpool.Arrays;

public class PrintUtils {
	
	public static void p (Object line) {
		System.out.println(line);
	}
	
	public static void pa(String[] array) {
		System.out.println(Arrays.toString(array));
	}
	
	public static <T> void pal(ArrayList<T> array) {
		for(Object o : array) {
			System.out.println(o.toString());
		}
	}
	
	public static void ph(HashSet<String> hashset) {
		System.out.println(hashset.toString());
	}

}
