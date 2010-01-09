package edu.unika.aifb.ease.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Util {
	public static class Counter {
		public int val = 0;
	}

	private static MessageDigest m_md;
	
	static {
		try {
			m_md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	synchronized public static long hash(String uri) {
		byte[] hash = m_md.digest(uri.getBytes());
		return new BigInteger(new byte[] {hash[14], hash[12], hash[10], hash[8], hash[6], hash[4], hash[2], hash[0]}).longValue();
	}
	
	public static String digest(String uri) {
		try{
			MessageDigest algorithm = MessageDigest.getInstance("MD5");
			algorithm.update(uri.getBytes());
			byte messageDigest[] = algorithm.digest();
		            
			StringBuffer hexString = new StringBuffer();
			for (int i=0;i<messageDigest.length;i++) {
				hexString.append(Integer.toHexString(0xFF & messageDigest[i]));
			}
			return "_" + hexString.toString(); 
		} catch(NoSuchAlgorithmException nsae){
		}
		
		return uri.replaceAll("\\/|:|\\.|#|\\?|&|\\+|-|~", "_");
	}
	
	public static final int bytesToInt(byte[] b) {
		return b[0]<<24 | (b[1]&0xff)<<16 | (b[2]&0xff)<<8 | (b[3]&0xff);
	}
	
	public static final byte[] intToBytes(int i) {
		return new byte[] { (byte)(i>>24), (byte)(i>>16), (byte)(i>>8), (byte)i };
	}
	
	public static String truncateUri(String uri) {
		if (uri == null)
			return null;
		int idx = uri.lastIndexOf("#");
		if (idx < 0)
			idx = uri.lastIndexOf("/");
		if (idx < 0)
			return uri;
		return uri.substring(idx + 1);
	}
	
//	public static void printDOT(Writer out, Graph graph) {
//		try {
//			out.write(graph.toDot());
//			out.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	public static void printDOT(String file, Graph graph) {
//		try {
//			printDOT(new FileWriter(file), graph);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//
	
	public static String resolveNamespace(String uri, Map<String,String> namespaces) {
		for (String ns : namespaces.keySet()) {
			if (uri.startsWith(ns + ":")) {
				return uri.replaceFirst(ns + ":", namespaces.get(ns));
			}
		}
		return uri;
	}
	
	public static Set<String> readEdgeSet(File file) {
		Set<String> edgeSet = new HashSet<String>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			String input;
			Map<String,String> namespaces = new HashMap<String,String>();
			while ((input = in.readLine()) != null) {
				input = input.trim();
				
				if (input.startsWith("ns:")) {
					String[] t = input.split(" ");
					namespaces.put(t[1], t[2]);
					continue;
				}
				
				edgeSet.add(resolveNamespace(input, namespaces));
			}
			in.close();
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}
		return edgeSet;
	}
	
	public static void writeEdgeSet(File file, Set<String> set) throws IOException {
		PrintWriter out = new PrintWriter(new FileWriter(file));
		for (String s : set)
			out.println(s);
		out.close();
	}
	
	public static Set<String> readEdgeSet(String file) {
		return readEdgeSet(new File(file));
	}
	
	public static void mergeRowSet(File file, Set<String> set) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			String input;
			while ((input = in.readLine()) != null) {
				input = input.trim();
				set.remove(input);
			}
			in.close();
			
			PrintWriter pw = new PrintWriter(new FileWriter(file, true));
			for (String row : set)
				pw.println(row);
			pw.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeObject(String file, Object object) throws FileNotFoundException, IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
		out.writeObject(object);
		out.close();
	}

	public static boolean pathContains(List<String> path, String source, String label, String target) {
		for (int i = 0; i < path.size() - 2; i += 2) {
			if (path.get(i).equals(source)) {
				if (path.get(i + 1).equals(label) && path.get(i + 2).equals(target))
					return true;
			}
		}
		return false;
	}

	public static Object readObject(String string) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(string));
		Object o = ois.readObject();
		ois.close();
		return o;
	}
	
	public static long freeMemory() {
		Runtime r = Runtime.getRuntime();
		return (r.freeMemory() + (r.maxMemory() - r.totalMemory())) / 1000;
	}
	
	public static String memory() {
		Runtime r = Runtime.getRuntime();
		long max = r.maxMemory() / 1000;
		return "memory (used/free/max): " + (max - freeMemory()) / 1000 + "/" + freeMemory() / 1000 + "/" + r.maxMemory() / 1000000;
	}
	
	public static boolean belowMemoryLimit(int percentFree) {
		long max = Runtime.getRuntime().maxMemory() / 1000;
		long free = freeMemory();

		if ((double)free / max * 100 < percentFree)
			return true;
		return false;
	}

	public static Object[] permute(int k, Object[] os) {
		for (int j = 2; j <= os.length; j++) {
			k = k / (j - 1);
			Object tmp = os[j - 1];
			os[j - 1] = os[(k % j)];
			os[(k % j)] = tmp;
		}
		return os;
	}

	public static int factorial(int n)
	{
	    if( n <= 1 )
	        return 1;
	    else
	        return n * factorial( n - 1 );
	}
	
//	public static Graph loadGT(String file) throws IOException {
//		Graph g = GraphFactory.graph();
//		
//		BufferedReader in = new BufferedReader(new FileReader(file));
//		String input;
//		while ((input = in.readLine()) != null) {
//			String[] t = input.split("\t");
//			g.addEdge(t[0], t[1], t[2]);
//		}
//		return g;
//	}

	public static void sortFile(String file, String fileOut) throws IOException, InterruptedException {
		Process p = Runtime.getRuntime().exec("sort -o " + fileOut + " " + file);
		p.waitFor();
		
//		LineSortFile lsf = new LineSortFile(file, fileOut);
//		lsf.setDeleteWhenStringRepeated(true);
//		lsf.sortFile();
	}

	public static <V> String atos(V[] a) {
		String s = "[";
		String comma = "";
		for (V v : a) {
			s += comma + v;
			comma = ",";
		}
		return s + "]";
	}

	public static boolean isVariable(String label) {
		return label.startsWith("?");
	}

	public static boolean isConstant(String label) {
		return !isVariable(label);
	}
	
	public static boolean isDataValue(String label) {
		return !isEntity(label);
	}
	
	public static boolean isEntity(String label) {
		return label.startsWith("http") || label.startsWith("_:") || label.startsWith("ttp://");
	}
}
