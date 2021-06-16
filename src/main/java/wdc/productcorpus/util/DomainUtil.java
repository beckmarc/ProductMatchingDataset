package wdc.productcorpus.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.net.InternetDomainName;

public class DomainUtil {

	public static String INVALID_URL = null;
	public static String blogspotPattern = "(.*\\.)(blogspot.)(.*)";

	/**
	 * This class gets the PLD from an URL and makes sure that all blogspot.com urls are
	 * mapped to the blogspot.com pld.
	 * @param url the URL
	 * @return the PLD
	 */
	public static String getPayLevelDomainFromWholeURL(String url) {
//		String domain = getDomain(url);
		try {
			String pld = getDomainName(url);
			// This is a necessary fix to guarantee blogspot.com is one PLD and
			// not millions
			//but also think of blogspot.de,.gr etc. (issue appeared for 2017 extraction)
//			String pld = fullDomainName.topPrivateDomain().toString();
//			Pattern r  = Pattern.compile(blogspotPattern);
//			Matcher m = r.matcher(pld);
//			if (m.find()) {
//				pld = m.group(2)+m.group(3);
//			}
////			if (pld.endsWith("blogspot.com")) {
////				pld = "blogspot.com";
////			}
			return pld;
		} catch (Exception e) {

		}
		return INVALID_URL;
	}

	private static final Pattern DOMAIN_PATTERN = Pattern.compile("http(s)?://(([a-zA-Z0-9-_]+(\\.)?)+)");

//	private static String getDomain(String uri) {
//		try {
//			Matcher m = DOMAIN_PATTERN.matcher(uri);
//			if (m.find()) {
//				return m.group(2);
//			}
//		} catch (Exception e) {
//
//		}
//		return uri;
//	}
//	
//	public static String getUrlDomain(String url) throws URISyntaxException {
//	    URI uri = new URI(url);
//	    String domain = uri.getHost();
//	    String[] domainArray = domain.split("\\.");
//	    if (domainArray.length == 1) {
//	        return domainArray[0];
//	    }
//	    return domainArray[domainArray.length - 2] + "." + domainArray[domainArray.length - 1];
//	}
	
	public static String getDomainName(String url) throws URISyntaxException {
	    URI uri = new URI(url);
	    String domain = uri.getHost();
	    return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

	public static void main (String args[]){
		
		//test the pld - blogspot thing
		String url1 = "http://fruitariangoldendiet.blogspot.de/2017/05/blog-post_25.html";
		String url2 = "http://javaarchramble.blogspot.com/2015/02/rabbitmq-and-openstack.html";
		String url3 = "http://javaarchramble.blogspot.de.com/2015/02/rabbitmq-and-openstack.html";
		String url4 = "http://blogspot.com";
		String url5 = "http://ilovemy.pet/products/sleeping-border-collie-floor-mat";
		System.out.println("URL 1 PLD:"+getPayLevelDomainFromWholeURL(url1));
		System.out.println("URL 2 PLD:"+getPayLevelDomainFromWholeURL(url2));
		System.out.println("URL 3 PLD:"+getPayLevelDomainFromWholeURL(url3));
		System.out.println("URL 4 PLD:"+getPayLevelDomainFromWholeURL(url4));
		System.out.println("URL 5 PLD:"+getPayLevelDomainFromWholeURL(url5));



	}
}
