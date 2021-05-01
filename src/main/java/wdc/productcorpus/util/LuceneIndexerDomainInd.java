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
import java.util.Arrays;
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
import org.apache.lucene.queryparser.classic.ParseException;
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
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import scala.collection.immutable.NumericRange;

public class LuceneIndexerDomainInd {

	private static final String INDEX_DIR = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters\\index_multiplefields\\0";
	private static final String indexFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters\\0.csv";
	
	private static final String searchFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters\\1.csv";
	private static final String outputFile = "C:\\Users\\User\\Desktop\\multisourceER_data\\voters\\pairsafterblocking\\0_1.csv";
	
	private static int hitsCount = 10;
	private static int idField = 0;
	private static Integer [] ignoreFields = new Integer[]{0};
	public static void main (String args[]) throws Exception{
		System.out.println("Start");
		LuceneIndexerDomainInd indexer = new LuceneIndexerDomainInd();
		//indexer.index();
		
		// now search
		//indexer.search();
		
		IndexSearcher searcher = createSearcher();

		
		TopDocs foundDocs = indexer.searchByConcatDesc("kristina perry winston salem 27106", searcher);
		for (ScoreDoc sd : foundDocs.scoreDocs) 
        {
            Document d = searcher.doc(sd.doc);
            System.out.println(d.get("desc"));
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
				
				String id = fields[idField];
				String concatDesc = "";
				for (Integer i=0; i<fields.length;i++) {
					if (i==idField || Arrays.asList(ignoreFields).contains(i)) continue;
					if(fields[i] != null && !fields[i].isEmpty()){
						concatDesc+=" "+fields[i];
					}
				}
				
				concatDesc = concatDesc.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
				concatDesc = concatDesc.replaceAll("[^\\s\\p{L}\\p{N}]+", " ").toLowerCase();
							
				TopDocs foundDocs = searchByConcatDesc(concatDesc, searcher);
				
				for (ScoreDoc sd : foundDocs.scoreDocs) 
		        {
		            Document d = searcher.doc(sd.doc);
		            System.out.println(concatDesc+"####"+d.get("desc"));
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
	private TopDocs searchByConcatDesc(String concatDesc, IndexSearcher searcher) throws IOException, ParseException {
		//Term term = new Term("desc", concatDesc);
		//Query query = new FuzzyQuery(term, 1);
		Query query = new QueryParser("desc", new StandardAnalyzer()).parse(concatDesc);
		TopDocs hits = searcher.search(query, hitsCount);
		
	    return hits;
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
			String id = fields[idField];
			String concatDesc = "";
			for (Integer i=0; i<fields.length;i++) {
				if (i==idField || Arrays.asList(ignoreFields).contains(i)) continue;
				if(fields[i] != null && !fields[i].isEmpty()){
					concatDesc+=" "+fields[i];
				}
			}
			
			concatDesc = concatDesc.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
			concatDesc = concatDesc.replaceAll("[^\\s\\p{L}\\p{N}]+", " ").toLowerCase();
			
			
			Document doc = createDocument(id,concatDesc);
			documents.add(doc);
		}
		reader.close();
		writer.deleteAll();
        
        writer.addDocuments(documents);
        writer.commit();
        writer.close();
	}
	
	private Document createDocument(String id, String concatDesc) {
		Document document = new Document();
	    document.add(new StringField("id", id.toString() , Field.Store.YES));	    
	    document.add(new TextField("desc", concatDesc , Field.Store.YES));	    
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
	
		
}