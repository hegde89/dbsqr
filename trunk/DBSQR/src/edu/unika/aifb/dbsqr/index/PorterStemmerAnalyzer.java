package edu.unika.aifb.dbsqr.index;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

public class PorterStemmerAnalyzer extends StandardAnalyzer {
	
	public PorterStemmerAnalyzer() {
		super();
	}
	
	public PorterStemmerAnalyzer(Version matchVersion) {
		super(matchVersion);
	}
	
	public PorterStemmerAnalyzer(File stopwords) throws IOException {
		super(stopwords);
	}

	public PorterStemmerAnalyzer(Version matchVersion, File stopwords) throws IOException {
		super(matchVersion, stopwords);
	}
	
	public TokenStream tokenStream(String field, Reader reader) {
		return new PorterStemFilter(super.tokenStream(field, reader));
	}
	
	public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
		return tokenStream(fieldName, reader);
	}

}
