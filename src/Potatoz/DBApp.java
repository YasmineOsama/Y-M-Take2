package Potatoz;

import java.io.BufferedReader;
import java.io.EOFException;
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
import java.time.Instant;
import java.util.*;

public class DBApp {
	static PrintWriter writer;
	static StringBuilder sb;
	File location;
	static Hashtable<String, Boolean> tableNames;
	static Hashtable<String, LinkedList<File>> files;
	static Hashtable<String, LinkedList<Page>> pages;
	static Hashtable<String, HashMap<String, LinkedList<Object>>> unikeys;

	public static boolean contains(Object[] objs, Object obj) {
		for (int i = 0; i < objs.length; i++) {
			if (obj.equals(objs))
				return true;
		}
		return false;
	}

	public void init() throws IOException {
		new File("data").mkdirs();
		new File("data/metadata.csv").createNewFile();
		new File("docs").mkdirs();
		new File("classes").mkdirs();
		new File("config").mkdirs();
		new File("config/DBApp.config").createNewFile();
		new File("classes/tables.class").createNewFile();
		/*
		 * Folders and files added for the beginning of the database
		 * initialization.
		 */
		Properties prop = new Properties();
		prop.setProperty("MaximumRowsCountinPage", "200");
		prop.setProperty("BRINSize", "15");
		prop.store(new FileOutputStream("config/DBApp.config"),
				"MaximumRowsCountinPage as the name indicates specifies the maximum number of rows in a page. BRINSize specifies the count of values that could be stored in a single BRIN file.");
		tableNames = new Hashtable<String, Boolean>();
		String title = "Table Name, Column Name, Column Type, Key, Indexed";
		String fileString = "data/metadata.csv";
		StringBuilder sb2 = new StringBuilder();
		sb2.append(title);
		FileWriter fw = new FileWriter(fileString, true);
		fw.write(sb2.toString());
		fw.write("\n");
		fw.close();
		files = new Hashtable<String, LinkedList<File>>();
		pages = new Hashtable<String, LinkedList<Page>>();
		unikeys = new Hashtable<String, HashMap<String, LinkedList<Object>>>();
	}

	public static void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws IOException, ClassNotFoundException {
		if (tableExists(strTableName)) {
			System.out.println("Table already exists");
		} else {
			LinkedList<File> tempfileList = new LinkedList<File>(); // LinkedList
																	// keeping
																	// track for
																	// all
																	// .class
																	// files for
																	// this
																	// table.
			LinkedList<Page> temppageList = new LinkedList<Page>(); // LinkedList
																	// keeping
																	// track for
																	// all pages
																	// created
																	// for this
																	// table.
			Couple[] row = new Couple[htblColNameType.size() + 1]; // Adding
																	// columns
																	// titles.
			Enumeration<String> keys = htblColNameType.keys();
			row[0] = new Couple(); // Couple is a class with two instance
									// variables, a key pointing to a value.
			row[0].setKey(strClusteringKeyColumn);
			row[0].setValue(htblColNameType.get(strClusteringKeyColumn));
			for (int i = 1; keys.hasMoreElements() && i < htblColNameType.size();) {
				String key = keys.nextElement();
				if (key != strClusteringKeyColumn) {
					row[i] = new Couple();
					row[i].setKey(key);
					row[i].setValue(htblColNameType.get(key));
					i++;
				}
			}
			row[htblColNameType.size()] = new Couple();
			row[htblColNameType.size()].setKey("TouchDate");
			row[htblColNameType.size()].setValue("java.time.Instant");
			new File("classes/" + strTableName + ".class").createNewFile(); // Contains
																			// info
																			// about
																			// the
																			// table's
																			// metadata.
			tempfileList.add(new File("classes/" + strTableName + "Data.class")); // File
																					// added
																					// in
																					// the
																					// list
																					// of
																					// files
																					// for
																					// the
																					// table.
			Page tempPage = new Page();
			temppageList.add(tempPage);
			files.put(strTableName, tempfileList);
			pages.put(strTableName, temppageList);
			HashMap<String, LinkedList<Object>> tempunikey = new HashMap<String, LinkedList<Object>>();
			tempunikey.put(strClusteringKeyColumn, new LinkedList<Object>());
			unikeys.put(strTableName, tempunikey); // Unique keys for each
													// record saved for futur
													// insertions.
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream("classes/" + strTableName + ".class", true));
			oos.writeObject(row); // Record saved in the .class file. Couple[]
									// which represents the restrictions which
									// gets written on the disk.
			oos.close();

			String keyType = htblColNameType.get(strClusteringKeyColumn); // Metadata
																			// part.
			Set<String> temp = ((Hashtable<String, String>) htblColNameType.clone()).keySet();
			temp.remove(strClusteringKeyColumn);
			sb = new StringBuilder();
			sb.append(strClusteringKeyColumn);
			sb.append(",");
			String htbl = temp.toString();
			htbl = htbl.substring(1, htbl.length() - 1);
			sb.append(htbl);
			List<String> roww = Arrays.asList(sb.toString().replaceAll("\\s+", "").split(","));
			for (int i = 0; i < roww.size(); i++) {
				String type = "";
				if (strClusteringKeyColumn.equals(roww.get(i))) {
					type = keyType;
				} else {
					type = htblColNameType.get(roww.get(i));
				}
				metadata(strTableName, roww.get(i), type, strClusteringKeyColumn.equals(roww.get(i)), true);
			}

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

	@SuppressWarnings("unchecked")
	public static boolean tableExists(String strTableName) throws IOException, ClassNotFoundException {
		List<Hashtable<String, Boolean>> tables = new ArrayList<Hashtable<String, Boolean>>();

		FileInputStream fis = null;
		try {
			fis = new FileInputStream("classes/tables.class");
			while (true) {
				ObjectInputStream ois = new ObjectInputStream(fis);
				tables.add((Hashtable<String, Boolean>) ois.readObject());
			}
		} catch (EOFException ignored) {
			// as expected
		} finally {
			if (fis != null)
				fis.close();
		}
		for (int i = 0; i < tables.size(); i++) {
			if (tables.get(i).containsKey(strTableName)) {
				return true;
			}
		}
		tableNames.put(strTableName, true);
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream("classes/tables.class");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(tableNames);
			oos.close();
		} finally {
			if (fos != null)
				fos.close();
		}
		return false;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/" + strTableName + ".class");
		ObjectInputStream ois = new ObjectInputStream(fis);
		Couple[] tableData = (Couple[]) (ois.readObject()); // Table's
															// keys
															// and
															// types
															// for
															// insertion
															// check.
		Couple[] rowData = new Couple[tableData.length]; // Array of Couples to
															// be passed and
															// written on the
															// disk.
		HashMap<String, LinkedList<Object>> tempunikey = unikeys.get(strTableName);
		String primkeys = (String) unikeys.get(strTableName).keySet().toArray()[0];
		LinkedList<Object> objs = tempunikey.get(primkeys);
		LinkedList<Page> p = pages.get(strTableName);
		LinkedList<File> f = files.get(strTableName);
		Page tempPage = pages.get(strTableName).getLast();
		File tempFile = files.get(strTableName).getLast();
		if (tempPage.full()) {
			tempPage = new Page();
			p.add(tempPage); // New page created and added at the end of the
								// LinkedList concerning the table.
			tempFile = new File("classes/" + strTableName + "Data" + f.size() + ".class");
			f.add(tempFile); // New .class file added at the end of the
								// LinkedList concerning the table.
		}
		Object[] keys = htblColNameValue.keySet().toArray();
		Object value;
		for (int i = 0; i < keys.length; i++) { // Assuring each column in the
												// record has a valid type.
			value = htblColNameValue.get(keys[i]);
			Couple tempCoup = null;
			for (int j = 0; j < tableData.length; j++) {
				if (keys[i].equals(tableData[j].getKey())) {
					tempCoup = tableData[j];
					break;
				}
			}
			if (!value.getClass().toString().substring(6).equals(tempCoup.getValue().toString())) {
				return;
			}
			rowData[i] = new Couple();
			rowData[i].setKey((String) keys[i]);
			rowData[i].setValue(value);
		}
		rowData[rowData.length - 1] = new Couple();
		rowData[rowData.length - 1].setKey("TouchDate");
		rowData[rowData.length - 1].setValue(Instant.now());
		for (Object object : objs) { // Assuring each record has a unique key in
			// the entire table.
			if (htblColNameValue.get(primkeys).equals(object)) {
				System.out.println("Record with a similar id exists.");
				return;
			}
		}
		objs.add(htblColNameValue.get(primkeys)); // Record's key added to
													// the LinkedList<Object>.
		tempPage.add(rowData); // Record added to the page.
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(files.get(strTableName).getLast(), true));
		oos.writeObject(tempPage); // Record saved in the .class file.
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
	// public void modify(String strTableName, String s) throws IOException {
	// List<String> lines = new ArrayList<String>();
	// String line = "";
	// File f1 = new File(strTableName + ".class");
	// FileReader fr = new FileReader(f1);
	// BufferedReader br = new BufferedReader(fr);
	// while ((line = br.readLine()) != null) {
	// lines.add(line);
	// }
	// LinkedList<Double> ids = new LinkedList<Double>();
	// for (int i = 1; i < lines.size(); i++) {
	// ids.add(Double.parseDouble(lines.get(i).split(",")[0]));
	// }
	// ids.add(Double.parseDouble(s.split(",")[0]));
	// Collections.sort(ids);
	// fr.close();
	// br.close();
	// writer = new PrintWriter("data/" + strTableName + ".csv");
	// String whole = lines.get(0) + '\n';
	// String[] temp = new String[ids.size()];
	// if (lines.size() > 1)
	// for (int i = 0; i < ids.size();)
	// for (int j = 1; j < lines.size() || !s.equals(""); j++) {
	// if (s.split(",")[0].equals(ids.get(i) + "")) {
	// whole += s.toString() + '\n';
	// s = "";
	// ids.remove(i);
	// break;
	// } else {
	// temp = lines.get(j).split(",");
	// if (temp[0].equals(ids.get(i) + "")) {
	//
	// whole += lines.get(j) + '\n';
	// lines.remove(j);
	// ids.remove(i);
	// break;
	// }
	// }
	// }
	// else
	// whole += s.toString() + '\n';
	//
	// writer.write(whole);
	// writer.close();
	// }

	public static void main(String[] args) throws IOException, ClassNotFoundException {
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
		htblColNameValue.clear();
		// htblColNameValue.put("id", new Integer(23498));
		// htblColNameValue.put("name", new String("John Noor"));
		// htblColNameValue.put("gpa", new Double(1.5));
		// app.insertIntoTable(strTableName, htblColNameValue);
		// htblColNameValue.clear();
		// htblColNameValue.put("id", new Integer(78452));
		// htblColNameValue.put("name", new String("Zaky Noor"));
		// htblColNameValue.put("gpa", new Double(0.88));
		// app.insertIntoTable(strTableName, htblColNameValue);
		// System.out.println(Instant.now().getClass());
	}

}
