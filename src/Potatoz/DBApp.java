package Potatoz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.*;

public class DBApp {
	static PrintWriter writer;
	static StringBuilder sb;
	File location;
	static HashMap<String, Hashtable<String, String>> restrictions;
	static Hashtable<String, File> files;
	static Hashtable<String, LinkedList<Page>> pages;

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
		File f1 = new File(strTableName + ".class");
		FileReader fr = new FileReader(f1);
		BufferedReader br = new BufferedReader(fr);
		while ((line = br.readLine()) != null) {
			lines.add(line);
		}
		LinkedList<Double> ids = new LinkedList<Double>();
		for (int i = 1; i < lines.size(); i++) {
			ids.add(Double.parseDouble(lines.get(i).split(",")[0]));
		}
		ids.add(Double.parseDouble(s.split(",")[0]));
		Collections.sort(ids);
		fr.close();
		br.close();
		writer = new PrintWriter("data/" + strTableName + ".csv");
		String whole = lines.get(0) + '\n';
		String[] temp = new String[ids.size()];
		if (lines.size() > 1)
			for (int i = 0; i < ids.size();)
				for (int j = 1; j < lines.size() || !s.equals(""); j++) {
					if (s.split(",")[0].equals(ids.get(i) + "")) {
						whole += s.toString() + '\n';
						s = "";
						ids.remove(i);
						break;
					} else {
						temp = lines.get(j).split(",");
						if (temp[0].equals(ids.get(i) + "")) {

							whole += lines.get(j) + '\n';
							lines.remove(j);
							ids.remove(i);
							break;
						}
					}
				}
		else
			whole += s.toString() + '\n';

		writer.write(whole);
		writer.close();
	}

	public void init() throws IOException {
		new File("data").mkdirs();
		new File("data/metadata.csv").createNewFile();
		new File("docs").mkdirs();
		new File("classes").mkdirs();
		new File("config").mkdirs();
		new File("config/DBApp.config").createNewFile();
		Properties prop1 = new Properties();
		prop1.setProperty("MaximumRowsCountinPage", "200");
		prop1.setProperty("BRINSize", "15");
		prop1.store(new FileOutputStream("config/DBApp.config"),
				"MaximumRowsCountinPage as the name indicates specifies the maximum number of rows in a page. BRINSize specifies the count of values that could be stored in a single BRIN file.");
		restrictions = new HashMap<String, Hashtable<String, String>>();
		String title = "Table Name, Column Name, Column Type, Key, Indexed";
		String fileString = "data/metadata.csv";
		StringBuilder sb2 = new StringBuilder();
		sb2.append(title);
		FileWriter fw = new FileWriter(fileString, true);
		fw.write(sb2.toString());
		fw.write("\n");
		fw.close();
		files = new Hashtable<String, File>();
		pages = new Hashtable<String, LinkedList<Page>>();
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws IOException {
		if (restrictions.containsKey(strTableName)) {
			System.out.println("Table already exists");
		} else {
			restrictions.put(strTableName, htblColNameType);
			String keyType = htblColNameType.get(strClusteringKeyColumn);
			files.put(strTableName, new File("data/" + strTableName + ".class"));
			LinkedList<Page> temp = new LinkedList<Page>();
			temp.add(new Page());
			pages.put(strTableName, temp);
			//			copy.put(keyType, htblColNameType.get(strClusteringKeyColumn));
			//			htblColNameType.remove(strClusteringKeyColumn);

			//			for(int i; i < htblColNameType.size(); i++) {
			//				htblColNameType.remove(strClusteringKeyColumn);
			//			}


			// sb = new StringBuilder();
			// sb.append(strClusteringKeyColumn);
			// sb.append(",");
			// String htbl = temp.toString();
			// htbl = htbl.substring(1, htbl.length() - 1);
			// sb.append(htbl);
			//
			// writer.write(sb.toString());
			// writer.close();
			files.put(strTableName, new File("data/" + strTableName +
					".class"));
			LinkedList<Page> tempList = new LinkedList<Page>();
			tempList.add(new Page());
			pages.put(strTableName, tempList);


			//List<String> col = Arrays.asList(sb.toString().replaceAll("\\s+", "").split(","));
//			for (int i = 0; i < col.size(); i++) {
//				String type = "";
//				if (strClusteringKeyColumn.equals(col.get(i))) {
//					type = keyType;
//				} else {
//					type = htblColNameType.get(col.get(i));
//				}
//				metadata(strTableName, col.get(i), type, strClusteringKeyColumn.equals(col.get(i)), false);
//				writeData(strTableName, strClusteringKeyColumn, htblColNameType);
//			}
		}

	}

	public static void metadata(String TableName, String ColName, String ColType, Boolean Key, Boolean index)
			throws IOException {
		String row = TableName + "," + ColName + "," + ColType + "," + Key + "," + index;
		String fileString = "data/metadata.csv";
		StringBuilder sb2 = new StringBuilder();
		sb2.append(row);
		FileWriter fw = new FileWriter(fileString, true);
		fw.write(sb2.toString());
		fw.write("\n");
		fw.close();

	}

	public void createBRINIndex(String strTableName, String strColName) {
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException {
		sb = new StringBuilder();
		LinkedList<Page> p = pages.get(strTableName);
		Page temp = (Page) (pages.get(strTableName).getLast());
		if (temp.full()) {
			temp = new Page();
			pages.get(strTableName).add(temp);
		}
		Object[] keys = htblColNameValue.keySet().toArray();
		Object value;
		for (int i = 0; i < keys.length; i++) {
			value = htblColNameValue.get(keys[i]);
			if (!value.getClass().toString().substring(6).equals(restrictions.get(strTableName).get(keys[i]))) {
				return;
			}
		}
		temp.add(htblColNameValue);
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(files.get(strTableName), true));
		oos.writeObject(temp);
		oos.close();
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
		// htblColNameValue.clear();
		// htblColNameValue.put("id", new Integer(23498));
		// htblColNameValue.put("name", new String("John Noor"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// app.insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		// htblColNameValue.put("id", new Integer(78452));
		// htblColNameValue.put("name", new String("Zaky Noor"));
		// htblColNameValue.put("gpa", new Double(0.88));
		// app.insertIntoTable(strTableName, htblColNameValue);
	}

}
