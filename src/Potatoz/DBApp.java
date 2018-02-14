package Potatoz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringBufferInputStream;
import java.util.*;

public class DBApp {
	static PrintWriter writer;
	static StringBuilder sb;
	File location;
	static HashMap<String, Hashtable<String, String>> restrictions;

	public static boolean contains(Object[] objs, Object obj) {
		for (int i = 0; i < objs.length; i++) {
			if (obj.equals(objs))
				return true;
		}
		return false;
	}

	public void modify(String strTableName, String s) throws IOException {
		List<String> lines = new ArrayList<String>();
		String line = "";
		File f1 = new File("data/" + strTableName + ".csv");
		FileReader fr = new FileReader(f1);
		BufferedReader br = new BufferedReader(fr);
		while ((line = br.readLine()) != null) {
			if (line.contains("java"))
				line = line.replace("java", " ");
			lines.add(line);
		}
		fr.close();
		br.close();
		writer = new PrintWriter("data/" + strTableName + ".csv");
		String whole = "";
		for (String string : lines) {
			whole += string + '\n';
		}
		writer.write(whole + s.toString());
		writer.close();
	}

	public void init() throws IOException {
		new File("data").mkdirs();
		new File("data/metadata.csv").createNewFile();
		new File("docs").mkdirs();
		new File("classes").mkdirs();
		new File("config").mkdirs();
		restrictions = new HashMap<String, Hashtable<String, String>>();
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
		Hashtable<String, String> htblColNameType) throws IOException {
		String keyType = htblColNameType.get(strClusteringKeyColumn);
		htblColNameType.keySet().remove(strClusteringKeyColumn);
		writer = new PrintWriter(new File("data/"+strTableName + ".csv"));
		sb = new StringBuilder();
		// sb.append(strClusteringKeyColumn);
		// sb.append(",");
		String htbl = htblColNameType.keySet().toString();
		htbl = htbl.substring(1, htbl.length() - 1);
		sb.append(htbl);

		writer.write(sb.toString());
		writer.close();
		String title = "Table Name, Column Name, Column Type, Key, Indexed";
		String fileString = "data/metadata.csv";
		StringBuilder sb2 = new StringBuilder();
		sb2.append(title);
		FileWriter fw = new FileWriter(fileString,true);
		fw.write(sb2.toString());
		fw.write("\n");
		fw.close();
		
		List<String> col = Arrays.asList(sb.toString().replaceAll("\\s+", "").split(","));
		for(int i =0; i<col.size(); i++) {
			String type = "";
			if(strClusteringKeyColumn.equals(col.get(i))) {
				type = keyType;
			}
			else {
				type =  htblColNameType.get(col.get(i));
			}
			metadata(strTableName, col.get(i), type, strClusteringKeyColumn.equals(col.get(i)), true);
		}
		
	}
	
	public static void metadata(String TableName, String ColName, String ColType, Boolean Key, Boolean index) throws IOException {
		String row = TableName + "," + ColName + "," + ColType + "," + Key + "," + index;
		String fileString = "data/metadata.csv";
		StringBuilder sb2 = new StringBuilder();
		sb2.append(row);
		FileWriter fw = new FileWriter(fileString,true);
		fw.write(sb2.toString());
		fw.write("\n");
		fw.close();
		
	}


	public void createBRINIndex(String strTableName, String strColName) {
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException {

		sb = new StringBuilder();
		Object[] keys = htblColNameValue.keySet().toArray();
		Object value;
		for (int i = 0; i < keys.length; i++) {
			value = htblColNameValue.get(keys[i]);
			if (value.getClass().toString().substring(6).equals(restrictions.get(strTableName).get(keys[i]))) {
				sb.append(value);
				if (i < keys.length - 1)
					sb.append(",");
			} else
				return;
		}
		modify(strTableName, sb.toString());
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue) {
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) {
	}

	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) {
		return null;
	}

	public static void main(String[] args) throws IOException {
		DBApp app = new DBApp();
		app.init();
		String strTableName = "Students";
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		createTable(strTableName, "id", htblColNameType);

		app.createTable(strTableName, "id", htblColNameType);
		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", new Integer(2343432));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(5674567));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Double(1.25));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(23498));
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(78452));
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double(0.88));
		app.insertIntoTable(strTableName, htblColNameValue);
	}

}
