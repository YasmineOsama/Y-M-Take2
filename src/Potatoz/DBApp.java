package Potatoz;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class DBApp {
	static PrintWriter writer;
	static StringBuilder sb;
	File location;
	Hashtable<String, Hashtable<String, String>> restrictions;

	public void init() throws IOException {
		new File("data").mkdirs();
		new File("data/metadata.csv").createNewFile();
		new File("docs").mkdirs();
		new File("classes").mkdirs();
		new File("config").mkdirs();
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws IOException {
		String keyType = htblColNameType.get(strClusteringKeyColumn);
		htblColNameType.keySet().remove(strClusteringKeyColumn);
		writer = new PrintWriter(new File("data/"+strTableName + ".csv"));
		sb = new StringBuilder();
		sb.append(strClusteringKeyColumn);
		sb.append(",");
		String htbl = htblColNameType.keySet().toString();
		htbl = htbl.substring(1, htbl.length()-1);
		sb.append(htbl);
		writer.write(sb.toString());
		writer.close();
		String title = "Table Name, Column Name, Column Type, Key, Indexed";
		String fileString = "/Users/yasmineosama/Documents/workspace/Potatoz/data/metadata.csv";
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
		String fileString = "/Users/yasmineosama/Documents/workspace/Potatoz/data/metadata.csv";
		StringBuilder sb2 = new StringBuilder();
		sb2.append(row);
		FileWriter fw = new FileWriter(fileString,true);
		fw.write(sb2.toString());
		fw.write("\n");
		fw.close();
		
	}

	public void createBRINIndex(String strTableName, String strColName) {
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) {

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
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		htblColNameType.put("uni", "java.lang.String");
		
		app.createTable( strTableName, "id", htblColNameType );
	}

}
