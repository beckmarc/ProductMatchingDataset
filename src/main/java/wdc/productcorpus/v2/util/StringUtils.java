package wdc.productcorpus.v2.util;

import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import scala.Char;

public class StringUtils {

	/**
	 * Removes the first and last character of string. Input is trimmed before to remove whitespace.
	 * 
	 * Use: e.g. <https://schema.org> becomes https://schema.org
	 * 
	 * @param tag
	 */
	public static String removeFirstLast(String string) {
		if(string.length() >= 2) {
			return string.trim().substring(1, string.length()-1);
		} else {
			return "";
		}
	}
	
	/**
	 * Calculates normalized Levensthein similarity
	 * @param s1
	 * @param s2
	 * @return
	 */
	public static double levSim(String s1, String s2) {
		NormalizedLevenshtein nl = new NormalizedLevenshtein();
		return 1 - nl.distance(s1, s2);
	}
	
	public static boolean containsOnlyZeros(String s) {
		for(char c : s.toCharArray()) {
			if(c != '0') {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Checks wether a string contains any letters or digits
	 * @param name
	 * @return
	 */
	public static boolean hasAlphaDigit(String name) {
	    char[] chars = name.toCharArray();

	    for (char c : chars) {
	        if(Character.isLetter(c) || Character.isDigit(c)) {
	            return true;
	        }
	    }

	    return false;
	}
	
}
