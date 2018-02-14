package Potatoz;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class DBApp {
	PrintWriter writer;
	StringBuilder sb;
	File location;
	Hashtable<String, Hashtable<String, String>> restrictions;

	public void init() throws IOException {
		new File("data").mkdirs();
		new File("data/metadata.csv").createNewFile();
		new File("docs").mkdirs();
		new File("classes").mkdirs();
		new File("config").mkdirs();
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws FileNotFoundException {
		htblColNameType.keySet().remove(strClusteringKeyColumn);
		writer = new PrintWriter(new File(strTableName + ".csv"));
		sb = new StringBuilder();
		sb.append(strClusteringKeyColumn);
		sb.append(",");
		sb.append(htblColNameType.keySet().remove(strClusteringKeyColumn));
		writer.write(sb.toString());
		writer.close();
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
	}

}
