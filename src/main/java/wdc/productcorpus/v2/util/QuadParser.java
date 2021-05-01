package wdc.productcorpus.v2.util;

import com.github.jsonldjava.core.RDFDataset.BlankNode;

import ldif.entity.Node;
import ldif.runtime.Quad;
import scala.actors.threadpool.Arrays;

/**
 * Custom Parser for n-quads that are used in the WDC groups class-specific subsets
 * 
 * @see <a href="http://webdatacommons.org/structureddata/2020-12/stats/schema_org_subsets.html">http://webdatacommons.org/structureddata/2020-12/stats/schema_org_subsets.html</a>
 *	
 * @author Marc Becker
 *
 */
public class QuadParser {
	
	public static final String SUBJECT_REGEX = "(\s+(?=<))";
	public static final String PREDICATE_REGEX = "(?<=>)\s+";
	public static final String OBJECT_URL_REGEX = ""
			+ "((?<=_:node[a-z0-9]{14,19})(\s)(?=<))|" // match nodes preceeded by <website-url>
			+ "(((?<=(@.{0,100})|\"))(\s)(?=<))|"
			+ "((?<=>)(\s)(?=<))";
	// (\s+(?=<))|((?!<\")(?=>)\s+)(?!\")|((?=\")\s)||(\s+(?=_:node))
	
	

	public Quad parseQuadLine(String line) {
		Node subject;
		Node object;
		String predicate;
		String websiteUrl;
		
		line = line.trim().substring(0, line.length() - 1).trim(); // remove the seperator points
		
		// parse subject
		String[] arraySubj = line.split(SUBJECT_REGEX, 2);
		subject = arraySubj[0].startsWith("_:") 
					? Node.createBlankNode(arraySubj[0].trim().substring(2), null) 
					: Node.createUriNode(arraySubj[0].trim().substring(1, arraySubj[0].length()-1), null);
		
		// parse predicate
		String[] arrayPred = arraySubj[1].split(PREDICATE_REGEX, 2);
		predicate = arrayPred[0].trim();
		
		// parse object and website URL
		String[] arrayObjUrl = arrayPred[1].split(OBJECT_URL_REGEX, 2);
		
		if(arrayObjUrl.length == 2) {
			if(arrayObjUrl[0].startsWith("_:")) { // if object is a node
				object =  Node.createBlankNode(arrayObjUrl[0].trim().substring(2), null);
			}
			else if (arrayObjUrl[0].matches("\".*\"")) {
				object = Node.createLiteral(arrayObjUrl[0].trim().substring(1, arrayObjUrl[0].length()-1), null);
			}
			else {
				object = Node.createUriNode(arrayObjUrl[0].trim().substring(1, arrayObjUrl[0].length()-1), null);
			}
			websiteUrl = arrayObjUrl[1].replaceAll("<|/\s*>|>", "");
		}
		else { // if something went wrong
			object = Node.createLiteral("", null);
			websiteUrl = "";
			pa(arrayObjUrl);
			p(line);
		}
		
		Quad quad = new Quad(subject, predicate, object, websiteUrl);
//		p(quad);

		return quad;
	}
	
	public static void p (Object line) {
		System.out.println(line);
	}
	
	public static void pa(String[] array) {
		System.out.println(Arrays.toString(array));
	}

//	public static Quad parseQuadLine(String line) {
//		return null
//	}
}
