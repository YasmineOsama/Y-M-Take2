package Potatoz;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.*;

public class DBApp {
	static StringBuilder sb;
	static Hashtable<String, LinkedList<File>> files;
	static Hashtable<String, LinkedList<Page>> pages;

	public void init() throws IOException {
		new File("data").mkdirs();
		new File("data/metadata.csv").createNewFile();
		new File("docs").mkdirs();
		new File("classes").mkdirs();
		new File("config").mkdirs();
		new File("config/DBApp.config").createNewFile();
		new File("classes/metadata.class").createNewFile();
		/*
		 * Folders and files added for the beginning of the database
		 * initialization.
		 */
		Properties prop = new Properties();
		prop.setProperty("MaximumRowsCountinPage", "200");
		prop.setProperty("BRINSize", "15");
		prop.store(new FileOutputStream("config/DBApp.config"),
				"MaximumRowsCountinPage as the name indicates specifies the maximum number of rows in a page."
						+ " BRINSize specifies the count of values that could be stored in a single BRIN file.");
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
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws IOException, ClassNotFoundException {
		HashMap<String, Couple[]> metaData = new HashMap<String, Couple[]>();
		HashMap<String, Couple[]> tempMetaData = readMetaData();
		if(tempMetaData!=null) {
			metaData = tempMetaData;
		}

		if (tableExists(strTableName)) {
			System.out.println("Table already exists");
		} else {
			new File("classes/"+ strTableName + "files.class").createNewFile();
			new File("classes/"+ strTableName + "pages.class").createNewFile();

			LinkedList<File> tempfileList = new LinkedList<File>(); // LinkedList keeping track for all
			// .class files for this table.
			LinkedList<Page> temppageList = new LinkedList<Page>(); // LinkedList keeping track for all pages
			// created for this table.
			Couple[] row = new Couple[htblColNameType.size() + 2]; // Adding columns data.
			Enumeration<String> keys = htblColNameType.keys();
			row[0] = new Couple(); // Couple is a class with two instance variables, a key pointing to a value.
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
			row[htblColNameType.size() + 1] = new Couple();
			row[htblColNameType.size()].setKey("TouchDate");
			row[htblColNameType.size()].setValue("java.time.Instant");
			row[htblColNameType.size() +1].setKey("primaryKey");
			row[htblColNameType.size() +1].setValue(strClusteringKeyColumn);


			metaData.put(strTableName, row);
			File t = new File("classes/" + strTableName + ".class");
			t.createNewFile();
			tempfileList.add(t); // File added in the list
			// of files for the table.
			Page tempPage = new Page();
			temppageList.add(tempPage);
			files.put(strTableName, tempfileList);
			pages.put(strTableName, temppageList);
			HashMap<String, LinkedList<Object>> tempunikey = new HashMap<String, LinkedList<Object>>();
			tempunikey.put(strClusteringKeyColumn, new LinkedList<Object>());
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream("classes/metadata.class"));
			oos.writeObject(metaData); // Saves metadata in .class file for future easier access
			//oos.flush();
			oos.close();

			String keyType = htblColNameType.get(strClusteringKeyColumn); // Metadata
			// part.
			@SuppressWarnings("unchecked")
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
		HashMap<String, Couple[]> metaData = new HashMap<String, Couple[]>();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream("classes/metadata.class");
			while (true) {
				ObjectInputStream ois = new ObjectInputStream(fis);
				metaData = (HashMap<String, Couple[]>) ois.readObject();
			}
		} catch (EOFException ignored) {
			// as expected
		} finally {
			if (fis != null)
				fis.close();
		}
		if (metaData.containsKey(strTableName)) {
			return true;
		}
		return false;
	}

	public HashMap<String, Couple[]> readMetaData() throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/metadata.class");
		if(fis.available() > 1) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			HashMap<String, Couple[]> metaData = (HashMap<String, Couple[]>) (ois.readObject());
			ois.close();
			return metaData;
		}
		fis.close();
		return null;
	}
	public Hashtable<String, LinkedList<File>> readPageFiles(String strTableName) throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/"+ strTableName + "files.class");
		if(fis.available() > 1) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, LinkedList<File>> tempFiles = (Hashtable<String, LinkedList<File>>) (ois.readObject());
			ois.close();
			return tempFiles;
		}
		fis.close();
		return null;
	}

	public Hashtable<String, LinkedList<Page>> readPagePages(String strTableName) throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/"+ strTableName + "files.class");
		if(fis.available() > 1) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, LinkedList<Page>> tempPages = (Hashtable<String, LinkedList<Page>>) (ois.readObject());
			ois.close();
			return tempPages;
		}
		fis.close();
		return null;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, ClassNotFoundException {
		HashMap<String, Couple[]> metaData = readMetaData();
		if(!tableExists(strTableName)) {
			System.out.println("Table doesn't exist");
		}
		else {
			FileInputStream fis = new FileInputStream("classes/"+ strTableName + ".class");
			if(fis.available() > 1) {
				ObjectInputStream ois = new ObjectInputStream(fis);
				Page readPage = (Page) (ois.readObject());
				HashMap<Object, Couple[]> table = readPage.getPage();
				ois.close();
				Object[] pkeys = table.keySet().toArray();
				for(int i = 0; i < pkeys.length; i++) { // Assuring each record has a unique key in
					// the entire table.
					if (htblColNameValue.containsValue(pkeys[i])) {
						System.out.println("Record with a similar id exists.");
						return;
					}
				}
			}
			Hashtable<String, LinkedList<File>> tempFiles = readPageFiles(strTableName);
			if(tempFiles!=null) {
				files = tempFiles;
			}
			Hashtable<String, LinkedList<Page>> tempPages = readPagePages(strTableName);
			if(tempPages!=null) {
				pages = tempPages;
			}
			Couple[] tableData = metaData.get(strTableName); // Table's keys and types
			// for insertion check.
			Couple[] rowData = new Couple[tableData.length - 1]; // Array of Couples to
			// be passed and written on the disk.
			LinkedList<Page> p = pages.get(strTableName);
			LinkedList<File> f = files.get(strTableName);
			Page tempPage = pages.get(strTableName).getLast();
			File tempFile = files.get(strTableName).getLast();
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
			if (tempPage.full()) {
				tempPage = new Page();
				p.add(tempPage); // New page created and added at the end of the
				// LinkedList concerning the table.
				tempFile = new File("classes/" + strTableName + f.size() + ".class");
				f.add(tempFile); // New .class file added at the end of the
				// LinkedList concerning the table.
			}

			rowData[rowData.length - 1] = new Couple();
			rowData[rowData.length - 1].setKey("TouchDate");
			rowData[rowData.length - 1].setValue(Instant.now());

			Couple[] rowInfo = metaData.get(strTableName);
			Object keyIndex = null;
			Object primaryKey = null;

			for(int i = 0; i < rowInfo.length; i++) { //getting column name of primary key
				if(rowInfo[i].getKey().equals("primaryKey")) {
					keyIndex = rowInfo[i].getValue();
					break;
				}
			} 
			for(int i = 0; i < rowData.length; i++) { //getting primary key value
				if(rowData[i].getKey().equals(keyIndex)) {
					primaryKey = rowData[i].getValue();
					break;
				}
			}
			pages.put(strTableName, p);
			files.put(strTableName, f);
			tempPage.add(primaryKey, rowData); // Record added to the page.
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(files.get(strTableName).getLast()));
			oos.writeObject(tempPage); // Record saved in the .class file.
			oos.close();
			ObjectOutputStream oos1 = new ObjectOutputStream(new FileOutputStream("classes/"+ strTableName + "files.class"));
			oos1.writeObject(files); //Recording table's associated files info
			oos1.close();
			ObjectOutputStream oos11 = new ObjectOutputStream(new FileOutputStream("classes/"+ strTableName + "pages.class"));
			oos11.writeObject(pages); //recording all pages of this table
			oos11.close();
		}

	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue) {
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) {
	}

	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) {
		return null;
	}


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
		htblColNameValue.put("id", new Integer(22));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));
		app.insertIntoTable(strTableName, htblColNameValue);
		FileInputStream fis = new FileInputStream("classes/Students.class");
		ObjectInputStream ois = new ObjectInputStream(fis);
		Page p = (Page) ois.readObject();
		HashMap<Object, Couple[]> metaData = p.getPage() ;
		ois.close();
		if(metaData != null) {
			System.out.println(metaData.toString());
			//System.out.println(metaData[0].getValue());
		}
		Couple[] b = metaData.get(22);
		System.out.println(b[0].getKey());
		System.out.println(b[0].getValue());

		//		htblColNameValue.clear();
		//		htblColNameValue.put("id", new Integer(5674567));
		//		htblColNameValue.put("name", new String("Dalia Noor"));
		//		htblColNameValue.put("gpa", new Double(1.25));
		//		app.insertIntoTable(strTableName, htblColNameValue);
		//		htblColNameValue.clear();
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