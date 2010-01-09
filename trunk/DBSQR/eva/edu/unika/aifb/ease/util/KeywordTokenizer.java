package edu.unika.aifb.ease.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class KeywordTokenizer {
	
    private String buf;
    private Set<String> m_stopWords;
    private int m_index, m_length;

    public KeywordTokenizer(String s) {
        buf = s;
        m_length = buf.length();
        m_index = 0;
    }
    
    public KeywordTokenizer(String s, Set<String> stopWords) {
        buf = s;
        m_length = buf.length();
        m_stopWords = stopWords;
        m_index = 0;
    }

    public String next() {
        StringBuffer sb = new StringBuffer(15);
        char c;
        boolean purenumber = true;

        if (m_index >= m_length)
            return null;

        while (m_index < m_length) {
            c = buf.charAt(m_index);
            if (isalpha(c) || isdigit(c)) {
                break;
            }
            m_index++;
        }

        while (m_index < m_length) {
            c = buf.charAt(m_index);
            if (isalpha(c))
                purenumber = false;
            else if (!isdigit(c)) {
                if (purenumber) {
                    sb.delete(0, sb.length());
                    m_index++;
                    continue;
                }
                else
                    break;
            }

            sb.append(c);
            m_index++;
        }

        if (sb.length() > 0) {
            return sb.toString();
        }

        return null;
    }

    public List<String> getAllTerms() {
    	String term;
    	ArrayList<String> terms = new ArrayList<String>(); 
    	while((term = next()) != null) {
    		term = term.toLowerCase();
            if (m_stopWords.contains(term)){
            	continue;
            }
            terms.add(term);
    	}
    	
    	return terms;
    }
    
    private boolean isalpha(char x) {
        return (x >= 'A' && x <= 'Z') || (x >= 'a' && x <= 'z');
    }

    private boolean isdigit(char x) {
        return x >= '0' && x <= '9';
    }
}
