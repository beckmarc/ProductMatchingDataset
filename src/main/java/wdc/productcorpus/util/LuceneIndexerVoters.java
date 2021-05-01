package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import scala.collection.immutable.NumericRange;

public class LuceneIndexerVoters {

	private static final String INDEX_DIR = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters_domainD\\index_multiplefields\\0";
	private static final String indexFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters_domainD\\0.csv";
	
	private static final String searchFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters_domainD\\1.csv";
	private static final String outputFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters_domainD\\pairsafterblocking\\0_1.csv";
	
	private static int topHits = 10;
	public static void main (String args[]) throws Exception{
		System.out.println("Start");
		LuceneIndexerVoters indexer = new LuceneIndexerVoters();
		//indexer.index();
		
		// now search
		//indexer.search();
		
		IndexSearcher searcher = createSearcher(); 
		TopDocs foundDocs = searchByMultipleFields("bobbie", "oxendine", "rowland", "28383", searcher, 50);
		for (ScoreDoc sd : foundDocs.scoreDocs) 
        {
            Document d = searcher.doc(sd.doc);
            System.out.println(d.get("id")+" "+d.get("givename")+" "+d.get("surname")+" "+d.get("suburb")+" "+d.get("postcode"));
        }
        
	}
	
	public void search() throws IOException {
		IndexSearcher searcher = createSearcher();
				
		BufferedReader reader = new BufferedReader(new FileReader(new File(searchFile)));
		String line = "";
		int lineCounter = 0;
		ArrayList<String> similar = new ArrayList<String>();
		while ((line=reader.readLine())!=null) {
			try {
				lineCounter++;
				if (lineCounter==1) continue;
				if (lineCounter%10000==0) System.out.println(lineCounter);
				String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				String id = fields[0];
				String givenname = fields[1];
				String surname = fields[2];
				String suburb = fields[3];
				String postcode = fields[4];
				givenname = Normalizer.normalize(givenname, Normalizer.Form.NFD)	;
				givenname = givenname.replaceAll("[^\\p{ASCII}]", "");
				surname = Normalizer.normalize(surname, Normalizer.Form.NFD)	;
				surname = surname.replaceAll("[^\\p{ASCII}]", "");
				suburb = Normalizer.normalize(suburb, Normalizer.Form.NFD)	;
				suburb = suburb.replaceAll("[^\\p{ASCII}]", "");
				postcode = Normalizer.normalize(postcode, Normalizer.Form.NFD)	;
				postcode = postcode.replaceAll("[^\\p{ASCII}]", "");
				
							
				givenname = givenname.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
				surname = surname.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
				suburb = suburb.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
				postcode = postcode.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();

				
				TopDocs foundDocs = searchByMultipleFields(givenname, surname, suburb, postcode, searcher, topHits);
				
				for (ScoreDoc sd : foundDocs.scoreDocs) 
		        {
		            Document d = searcher.doc(sd.doc);
		            System.out.println(surname+"####"+d.get("surname"));
		            similar.add(d.get("id")+";"+id);
		        }
				
				//write the pairs
				if (similar.size()>100000){
					BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputFile),true));
					for (String pair:similar){
						writer.write(pair+"\n");
					}
					writer.flush();
					writer.close();
					similar.clear();
				}
				
			}
			catch(Exception e){
				System.out.println(e.getMessage());
				System.out.println(line);
			} 
			
		}
		reader.close();
		
		//write the pairs
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputFile),true));
		for (String pair:similar){
			writer.write(pair+"\n");
		}
		writer.flush();
		writer.close();
		
        
	}
	public void index() throws IOException{
		IndexWriter writer = createWriter();
        List<Document> documents = new ArrayList<>();
		BufferedReader reader = new BufferedReader(new FileReader(new File(indexFile)));
		String line = "";
		int lineCounter = 0;
		while ((line=reader.readLine())!=null) {
			lineCounter++;
			if (lineCounter==1) continue;
			if (lineCounter%10000==0) System.out.println(lineCounter);
			//which field do you want to use for indexing?
			String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			String id = fields[0];
			String givenname = fields[1];
			String surname = fields[2];
			String suburb = "";
			try{
				suburb = fields[3];
			}
			catch (Exception e){
				suburb ="";
			}
			
			
			String postcode = "";
			try{
				postcode = fields[4];
			}
			catch (Exception e){
				postcode ="";
			}
			givenname = Normalizer.normalize(givenname, Normalizer.Form.NFD)	;
			givenname = givenname.replaceAll("[^\\p{ASCII}]", "");
			surname = Normalizer.normalize(surname, Normalizer.Form.NFD)	;
			surname = surname.replaceAll("[^\\p{ASCII}]", "");
			suburb = Normalizer.normalize(suburb, Normalizer.Form.NFD)	;
			suburb = suburb.replaceAll("[^\\p{ASCII}]", "");			
			postcode = Normalizer.normalize(postcode, Normalizer.Form.NFD)	;
			postcode = postcode.replaceAll("[^\\p{ASCII}]", "");
			
			
			givenname = givenname.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
			surname = surname.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
			suburb = suburb.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
			postcode = postcode.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
			
			Document doc = createDocument(id, givenname, surname, suburb, postcode);
			documents.add(doc);
		}
		reader.close();
		writer.deleteAll();
        
        writer.addDocuments(documents);
        writer.commit();
        writer.close();
	}
	
	private static Document createDocument(String id, String givename, String surname, String suburb, String postcode) 
	{
	    Document document = new Document();
	    document.add(new StringField("id", id.toString() , Field.Store.YES));
	    
	    document.add(new StringField("givename", givename , Field.Store.YES));
	    document.add(new StringField("surname", surname , Field.Store.YES));
	    document.add(new StringField("suburb", suburb , Field.Store.YES));
	    document.add(new StringField("postcode", postcode , Field.Store.YES));
	    return document;
	}
	
	private static IndexWriter createWriter() throws IOException 
	{
	    FSDirectory dir = FSDirectory.open(Paths.get(INDEX_DIR));
	    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
	    IndexWriter writer = new IndexWriter(dir, config);
	    return writer;
	}
	
	private static IndexSearcher createSearcher() throws IOException 
	{
	    Directory dir = FSDirectory.open(Paths.get(INDEX_DIR));
	    IndexReader reader = DirectoryReader.open(dir);
	    IndexSearcher searcher = new IndexSearcher(reader);
	    return searcher;
	}
	
	private static TopDocs searchByMultipleFields(String givename, String surname, String suburb, String postcode, IndexSearcher searcher, int hitscount) throws Exception
	{
		
	    //fuzzy query for title which supports few edits on the character level
		QueryParser qp_name = new QueryParser("givename",  new StandardAnalyzer());
	    Query name_query = qp_name.parse(givename);
		
	    QueryParser qp_surname = new QueryParser("surname",  new StandardAnalyzer());
	    Query surname_query = qp_surname.parse(surname);

	    QueryParser qp_postcode = new QueryParser("postcode",  new StandardAnalyzer());
	    Query postcode_query = qp_postcode.parse(postcode);
	    
	    QueryParser qp_suburb = new QueryParser("suburb",  new StandardAnalyzer());
	    Query suburb_query = qp_suburb.parse(suburb);

	    BooleanQuery.Builder multipleFieldsQuery = new BooleanQuery.Builder();
		multipleFieldsQuery.add(name_query, Occur.MUST);
		multipleFieldsQuery.add(surname_query, Occur.MUST);
		multipleFieldsQuery.add(postcode_query, Occur.SHOULD);
		multipleFieldsQuery.add(suburb_query, Occur.SHOULD);

		
		BooleanQuery query = multipleFieldsQuery.build();
		TopDocs hits = searcher.search(query, hitscount);
		
		return hits;
	}
	
	
	



	
}