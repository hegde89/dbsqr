import static org.junit.Assert.*;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestLucene {
	
	private static Logger log  = Logger.getLogger(TestLucene.class); 
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		// Store the index on disk:
		File dir = new File("d://test/lucene");
		if(!dir.exists())
			dir.mkdirs();
		Directory directory = FSDirectory.open(dir);
		IndexWriter iwriter = new IndexWriter(directory, analyzer, true, new IndexWriter.MaxFieldLength(25000));
		for(int i = 0; i < 20; i++) {
			Document doc = new Document();
			String text = "I put the term" + i + " in the documents, I am Lei Zhang.";
			String annotion = "This is term" + i;
			doc.add(new Field("textfield", text, Field.Store.YES, Field.Index.ANALYZED));
			doc.add(new Field("textfield", text, Field.Store.YES, Field.Index.ANALYZED));
			doc.add(new Field("annotionfield", annotion, Field.Store.YES, Field.Index.NOT_ANALYZED));
			iwriter.addDocument(doc);
		}
		iwriter.close();
		directory.close();
	}
	
	@Before
	public void before() throws Exception {
		
	}
	
	@AfterClass
	public static void afterClass() throws Exception {
		
	} 
	
	@After
	public void after() throws Exception {
		
	}
	
	@Ignore
	@Test
	public void testSearch() throws Exception {
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		Directory directory = FSDirectory.open(new File("d://test/lucene"));
		// Now search the index:
		IndexSearcher isearcher = new IndexSearcher(directory, true); // read-only=true
		// Parse a simple query that searches for "text":
		QueryParser parser = new QueryParser("fieldname", analyzer);
		Query query = parser.parse("Lei");
		ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
		assertEquals(20, hits.length);
		// Iterate through the results:
		for (int i = 0; i < hits.length; i++) {
			Document hitDoc = isearcher.doc(hits[i].doc);
			log.debug(hitDoc.get("annotionfield"));
			log.debug(hitDoc.get("textfield"));
		}
		isearcher.close();
		directory.close();
	}
	
	@Ignore
	@Test
	public void testTerms() throws Exception {
		Directory directory = FSDirectory.open(new File("d://test/lucene"));
		IndexReader ireader = IndexReader.open(directory, true);
		TermEnum tEnum = ireader.terms();
		while(tEnum.next()) {
			Term term = tEnum.term();
			String field = term.field();
			String text = term.text();
			log.debug(field + ": " + text);
		}
		
		ireader.close();
		directory.close();
	}
	
//	@Ignore
	@Test
	public void testTermDocs() throws Exception {
		Directory directory = FSDirectory.open(new File("d://test/lucene"));
		IndexReader ireader = IndexReader.open(directory, true);
		TermEnum tEnum = ireader.terms();
		while(tEnum.next()) {
			Term term = tEnum.term();
			String field = term.field();
			String text = term.text();
			log.debug(text + ": ");
			
			TermDocs tDocs = ireader.termDocs(term);
			while(tDocs.next()) {
				int docID = tDocs.doc();
				int termFreq = tDocs.freq();
				log.debug("--- " + termFreq + " ---");
				Document doc = ireader.document(docID);
				text = doc.get("fieldname"); 
				log.debug(text);
			}
		}
		
		ireader.close();
		directory.close();
	}
	
}
