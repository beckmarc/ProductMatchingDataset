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

public class LuceneIndexerGeo {

	private static final String INDEX_DIR = "C:\\Users\\User\\Desktop\\multisourceER_data\\index_multiplefields\\httpsws.geonames.org";
	private static final String indexFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\httpsws.geonames.org.csv";
	
	private static final String searchFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\httprdf.freebase.com.csv";
	private static final String outputFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\pairsafterblocking\\httpsws.geonames.org_httprdf.freebase.com.csv";
	
	
	public static void main (String args[]) throws Exception{
		System.out.println("Start");
		LuceneIndexerGeo indexer = new LuceneIndexerGeo();
		//indexer.index();
		
		// now search
		//indexer.search();
		
		IndexSearcher searcher = createSearcher();

		TopDocs foundDocs = searchByMultipleFields("ho chi minh city",  10.75,  106.667, searcher, 10);
		for (ScoreDoc sd : foundDocs.scoreDocs) 
        {
            Document d = searcher.doc(sd.doc);
            System.out.println(d.get("label"));
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
				String id = fields[3];
				String label = fields[0];
				label = Normalizer.normalize(label, Normalizer.Form.NFD)	;
				label = label.replaceAll("[^\\p{ASCII}]", "");
				Double lat = -1.0; //if missing value get -1
				Double lon = -1.0;
				
				if(fields[1] != null && !fields[1].isEmpty())
					lat = Double.parseDouble(fields[1]);
				if(fields[2] != null && !fields[2].isEmpty())
					lon = Double.parseDouble(fields[2]);
				label = label.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
				
				
				if (label.equals(" ")) continue;
				//TopDocs foundDocs = searchByTitle(title, searcher, 20);

				TopDocs foundDocs = searchByMultipleFields(label, lat, lon, searcher, 20);
				
				for (ScoreDoc sd : foundDocs.scoreDocs) 
		        {
		            Document d = searcher.doc(sd.doc);
		            System.out.println(label+"####"+d.get("label"));
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
			String id = fields[3];
			String label = fields[0];
			label = Normalizer.normalize(label, Normalizer.Form.NFD)	;
			label = label.replaceAll("[^\\p{ASCII}]", "");
			Double lat = -1.0; //if missing value get -1
			Double lon = -1.0;
			
			if(fields[1] != null && !fields[1].isEmpty())
				lat = Double.parseDouble(fields[1]);
			if(fields[2] != null && !fields[2].isEmpty())
				lon = Double.parseDouble(fields[2]);
			Document doc = createDocument(id,label,lat,lon);
			documents.add(doc);
		}
		reader.close();
		writer.deleteAll();
        
        writer.addDocuments(documents);
        writer.commit();
        writer.close();
	}
	
	private static Document createDocument(String id, String label, Double lat, Double lon) 
	{
	    Document document = new Document();
	    document.add(new StringField("id", id.toString() , Field.Store.YES));
	    
	    label = label.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase();
	    document.add(new TextField("label", label , Field.Store.YES));	    
	    document.add(new org.apache.lucene.document.DoublePoint("lat", lat));
	    document.add(new org.apache.lucene.document.DoublePoint("lon", lon));

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
	
	private static TopDocs searchByMultipleFields(String label, double lat, double lon, IndexSearcher searcher, int hitscount) throws Exception
	{
		Query lat_query = org.apache.lucene.document.DoublePoint.newRangeQuery("lat", (lat-1.0), (lat+1.0));
		Query lon_query = org.apache.lucene.document.DoublePoint.newRangeQuery("lon", (lon-1.0), (lon+1.0));
		
	    //fuzzy query for title which supports few edits on the character level
		Term term = new Term("label", label);
		Query title_query_1 = new FuzzyQuery(term);
		
		//text field query with standard analyzer
		QueryParser qp = new QueryParser("label",  new StandardAnalyzer());
	    Query title_query_2 = qp.parse(label);
		
		BooleanQuery.Builder multipleFieldsQuery = new BooleanQuery.Builder();
		multipleFieldsQuery.add(lat_query, Occur.SHOULD);
		multipleFieldsQuery.add(lon_query, Occur.SHOULD);
		multipleFieldsQuery.add(title_query_1, Occur.SHOULD);
		multipleFieldsQuery.add(title_query_2, Occur.SHOULD);

		
		BooleanQuery query = multipleFieldsQuery.build();
		TopDocs hits = searcher.search(query, hitscount);
		
		return hits;
	}
	
	private static TopDocs searchByTitle(String title, IndexSearcher searcher, int hitscount) throws Exception
	{ 
		
		Term term = new Term("label", title);
		Query query = new FuzzyQuery(term, 1);
		TopDocs hits = searcher.search(query, hitscount);
	    return hits;
	}
	
	



	
}