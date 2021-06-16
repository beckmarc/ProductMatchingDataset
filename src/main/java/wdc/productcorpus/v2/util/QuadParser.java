package wdc.productcorpus.v2.util;

import ldif.entity.Node;
import ldif.runtime.Quad;
import scala.actors.threadpool.Arrays;
import wdc.productcorpus.v2.util.model.LanguageLiteral;
import wdc.productcorpus.v2.util.model.TypedLiteral;

/**
 * Custom Parser for n-quads that are used in the WDC groups class-specific subsets
 * 
 * @see <a href="http://webdatacommons.org/structureddata/2020-12/stats/schema_org_subsets.html">http://webdatacommons.org/structureddata/2020-12/stats/schema_org_subsets.html</a>
 *	
 * @author Marc Becker
 *
 */
public class QuadParser {
	
	public QuadParser() {
		this.debug = false;
	}
	
	public QuadParser(boolean debug) {
		this.debug = debug;
	}
	
	public boolean debug;
	public static final String SUBJECT_REGEX = "(\\s+(?=<))";
	public static final String PREDICATE_REGEX = "(?<=>)\\s+";
	public static final String OBJECT_URL_REGEX = "\\s(?=<https?)";
//			+ "((?<=_:node[a-z0-9]{14,19})(\s)(?=<))|" // match nodes preceeded by <website-url>
//			+ "(((?<=(@.{0,100})|\"))(\s)(?=<))|"
//			+ "((?<=>)(\s)(?=<))";
	// (\s+(?=<))|((?!<\")(?=>)\s+)(?!\")|((?=\")\s)||(\s+(?=_:node))


	public Quad parseQuadLine(String line) throws Exception {
		Node subject = null;
		Node object = null;
		String predicate = "";
		String websiteUrl = "";
		
		// remove the seperator points
		line = line.trim().substring(0, line.length() - 1).trim(); 
		
		// parse subject
		String[] arraySubj = line.split(SUBJECT_REGEX, 2);
//		if(arraySubj.length != 2) {
//			PrintUtils.p(line);
//			PrintUtils.pa(arraySubj);
//		}
		subject = parseSubject(arraySubj[0]);
		
		// parse predicate
//		if(arraySubj.length != 2) {
//			PrintUtils.p(line);
//			PrintUtils.pa(arraySubj);
//		}
			
		//PrintUtils.pa(arraySubj);
		
		String[] arrayPred = arraySubj[1].split(PREDICATE_REGEX, 2);
//		if(arrayPred.length != 2) {
//			PrintUtils.p(line);
//			PrintUtils.pa(arrayPred);
//		}
		predicate = parsePredicate(arrayPred[0]);
		
		// parse object and website URL
		// parse predicate
		//PrintUtils.pa(arraySubj);
		
		String[] arrayObjUrl = arrayPred[1].split(OBJECT_URL_REGEX, 2);
		if(arrayObjUrl.length != 2) {
//			PrintUtils.p(line);
//			PrintUtils.pa(arrayObjUrl);
		}
		//pa(arrayObjUrl);
		object = parseObject(arrayObjUrl[0]);
		websiteUrl = parseWebsiteUrl(arrayObjUrl[1]);
		
		Quad quad = new Quad(subject, predicate, object, websiteUrl);
//		p(quad);

		return quad;
	}
	
	/**
	 * Parses a String representation of an rdfa Object
	 * 
	 * 	object	::=	IRIREF | BLANK_NODE_LABEL | literal
	 * 	literal	::=	STRING_LITERAL_QUOTE ('^^' IRIREF | LANGTAG)?
	 * 
	 * @param objectString
	 * @return
	 */
	private Node parseObject(String objectString) {
		Node object = null;
		if(objectString.startsWith("_:")) { // if object is a node
			object =  Node.createBlankNode(objectString.trim().substring(2), null);
		}
		else if (objectString.matches("\".*\"")) { // if object is a pure literal value
			object = Node.createLiteral(StringUtils.removeFirstLast(objectString), null);
		}
		else if(LanguageLiteral.isType(objectString)) { // if object is a literal value with language addition e.g. @uk or @ru after the literal
			LanguageLiteral l = new LanguageLiteral(objectString);
			object = Node.createLanguageLiteral(l.getValue(), l.getLanguage(), null);
		}
		else if(objectString.matches("<.+>")){ // if object is URI node
			object = Node.createUriNode(StringUtils.removeFirstLast(objectString), null);
		} 
		else if(TypedLiteral.isType(objectString)) { // typed literal (date literal) "21-02-2021"^^
			TypedLiteral tl = new TypedLiteral(objectString);
			object = Node.createTypedLiteral(tl.getValue(), tl.getType(), null);
		}
		else { // if the object is malformed
			object = Node.createLiteral(StringUtils.removeFirstLast(objectString), null);
		}
		return object;
	}
	
	/**
	 * Generally the website URL is the graph label. Can be a IRIREF or BLANK_NODE_LABEL.
	 * 
	 * @param urlString
	 * @return the parsed website url
	 */
	private String parseWebsiteUrl(String urlString) {
		return urlString.trim().replaceAll("<|/\\s*>|>", "");
	}
	
	/**
	 * Predicate is technically of type IRIREF but the string is enough here
	 *  
	 * @param predicateString
	 * @return The parsed predicate string
	 */
	private String parsePredicate(String predicateString) {
		return predicateString.trim();
	}
	
	/**
	 * Parses a subject String
	 * 
	 * According to the documentation subject can be either: IRIREF or BLANK_NODE_LABEL
	 * 
	 * @param subjectString
	 * @return The parsed subject node
	 */
	private Node parseSubject(String subjectString) {
		subjectString = subjectString.trim();
		if(subjectString.startsWith("_:")) {
			return Node.createBlankNode(subjectString.substring(2), null);
		} 
		else {
			return Node.createUriNode(StringUtils.removeFirstLast(subjectString), null);
		}
	}
	
	public static Quad blankQuad() throws Exception {
		return new Quad(Node.createBlankNode("", null), "", Node.createBlankNode("", null), "");
	}

	
	private void throwException(String line) throws Exception {
		if(this.debug) {
			throw new Exception("Error in line detected. This is the malformed line: \n" + line);
		}
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

