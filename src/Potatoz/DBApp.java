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

	/**
	 * Reading existing metadata previously written on disk for further table
	 * creations.
	 * 
	 * Method with disk access to restore database checkpoints.
	 */
	public HashMap<String, Couple[]> readMetaData() throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/metadata.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			HashMap<String, Couple[]> metaData = (HashMap<String, Couple[]>) (ois.readObject());
			ois.close();
			return metaData;
		}
		fis.close();
		return null;
	}

	/**
	 * Initializing database.
	 * 
	 * Method creates folder directories for proper data organization and
	 * storing.
	 */
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
		/*
		 * Config file created.
		 */
		String title = "Table Name, Column Name, Column Type, Key, Indexed";
		String fileString = "data/metadata.csv";
		StringBuilder sb2 = new StringBuilder();
		sb2.append(title);
		FileWriter fw = new FileWriter(fileString, true);
		fw.write(sb2.toString());
		fw.write("\n");
		fw.close();
		/*
		 * Very first lines of metadata.csv put.
		 */
		files = new Hashtable<String, LinkedList<File>>();
		pages = new Hashtable<String, LinkedList<Page>>();
		/*
		 * Hashtables keeping track for every file and page created for all
		 * tables.
		 */
	}

	/**
	 * On new table creation.
	 * 
	 * @param strTableName:
	 *            Table name to be created.
	 * @param strClusteringKeyColumn:
	 *            Table column referring to the primary key of this table.
	 * @param htblColNameType:
	 *            Hashtable containing all columns with keys as the column names
	 *            and values as the data type accepted for each column.
	 */
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws IOException, ClassNotFoundException {
		HashMap<String, Couple[]> metaData = new HashMap<String, Couple[]>();
		HashMap<String, Couple[]> tempMetaData = readMetaData();
		if (tempMetaData != null) {
			metaData = tempMetaData;
		}
		/*
		 * Checking for existing previously saved metadata and extracting it,
		 * otherwise creating a new metadata tracker.
		 */
		if (tableExists(strTableName)) {
			System.out.println("From Database: Table already exists.");
		} else {
			new File("classes/" + strTableName + "files.class").createNewFile();
			new File("classes/" + strTableName + "pages.class").createNewFile();

			LinkedList<File> tempfileList = new LinkedList<File>();
			/*
			 * LinkedList keeping track for all .class files for this table.
			 */
			LinkedList<Page> temppageList = new LinkedList<Page>();
			/*
			 * LinkedList keeping track for all pages created for this table.
			 */
			Couple[] row = new Couple[htblColNameType.size() + 2];
			/*
			 * Adding columns data/restrictions.
			 */
			Enumeration<String> keys = htblColNameType.keys();
			row[0] = new Couple(); // Couple is a class with two instance
			// variables, a key pointing to a value.
			row[0].setKey(strClusteringKeyColumn); // Primary Key added as a
			// key.
			row[0].setValue(htblColNameType.get(strClusteringKeyColumn)); // Primary
			// Key's
			// restriction
			// added.

			/*
			 * Iterating over the tables restriction and transforming them to
			 * Couples<Key,Restriction>.
			 */
			for (int i = 1; keys.hasMoreElements() && i < htblColNameType.size();) {
				String key = keys.nextElement();
				if (key != strClusteringKeyColumn) {
					row[i] = new Couple();
					row[i].setKey(key);
					row[i].setValue(htblColNameType.get(key));
					i++;
				}
			}
			/*
			 * Adding additional column for TouchDate (Date of record
			 * insertion).
			 */
			row[htblColNameType.size()] = new Couple();
			row[htblColNameType.size()].setKey("TouchDate");
			row[htblColNameType.size()].setValue("java.time.Instant");

			/*
			 * Array of Couples<Key,Restriction> added to the hashmap of the
			 * metadata to keep track of the table's restrictions.
			 */
			metaData.put(strTableName, row);

			File t = new File("classes/" + strTableName + ".class");
			t.createNewFile();
			tempfileList.add(t); // File added in the list of files for the
			// table.
			Page tempPage = new Page();
			temppageList.add(tempPage); // Page added in the list of pages for
			// the table.

			/*
			 * Instance variables for the DataBase updated with the trackers for
			 * files <LinkedList> and pages <LinkedList> concerning this
			 * specific table in the (files <Hashtable>) and (pages <Hashtable>)
			 * variables.
			 */
			files.put(strTableName, tempfileList);
			pages.put(strTableName, temppageList);

			// HashMap<String, LinkedList<Object>> tempunikey = new
			// HashMap<String, LinkedList<Object>>();
			// tempunikey.put(strClusteringKeyColumn, new LinkedList<Object>());
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("classes/metadata.class"));
			/*
			 * Saves metadata in .class file for future easier access.
			 */
			oos.writeObject(metaData);
			// oos.flush();
			oos.close();

			/*
			 * metadata.csv part.
			 */
			String keyType = htblColNameType.get(strClusteringKeyColumn);
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

			ObjectOutputStream oos1 = new ObjectOutputStream(
					new FileOutputStream("classes/" + strTableName + "files.class"));
			oos1.writeObject(files); // Recording table's associated files info
			oos1.close();
			ObjectOutputStream oos11 = new ObjectOutputStream(
					new FileOutputStream("classes/" + strTableName + "pages.class"));
			oos11.writeObject(pages); // recording all pages of this table
			oos11.close();
			System.out.println("From Database: Table " + strTableName + " created and stored in the database.");
		}

	}

	/**
	 * Method responsible for metadata.csv creation.
	 * 
	 * @param TableName:
	 *            Table name to be written on the csv.
	 * @param ColName:
	 *            Column name to be written on the csv.
	 * @param ColType:
	 *            Column's content accepted data type to be written in the csv.
	 * @param Key:
	 *            Boolean indicating the primary key of the table.
	 * @param index:
	 *            Boolean indicating indexed content.
	 */
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

	/**
	 * Checker for previously similar created tables in the metadata.class.
	 * 
	 * @param strTableName:
	 *            Table name received by the method which has access to the disk
	 *            and re-reads the metadata to check if a table with similar
	 *            name is created.
	 */
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

	/**
	 * Method for getting all files contributing in all the tables' data, null
	 * if there's none.
	 * 
	 * @param strTableName:
	 *            Table name received by the method which has access to the disk
	 *            and return the stored written-on files for this table.
	 */
	public Hashtable<String, LinkedList<File>> readPageFiles(String strTableName)
			throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/" + strTableName + "files.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			Hashtable<String, LinkedList<File>> tempFiles = (Hashtable<String, LinkedList<File>>) (ois.readObject());
			ois.close();
			return tempFiles;
		}
		fis.close();
		return null;
	}

	/**
	 * Method for getting all pages contributing in all the tables' data, null
	 * if there's none.
	 * 
	 * @param strTableName:
	 *            Table name received by the method which has access to the disk
	 *            and return the stored written-on pages for this table.
	 */
	public Hashtable<String, LinkedList<Page>> readPagePages(String strTableName)
			throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/" + strTableName + "pages.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			Hashtable<String, LinkedList<Page>> tempPages = (Hashtable<String, LinkedList<Page>>) (ois.readObject());
			ois.close();
			return tempPages;
		}
		fis.close();
		return null;
	}

	/**
	 * On new record insertion.
	 * 
	 * @param strTableName:
	 *            Table name to be used for insertion.
	 * @param htblColNameValue:
	 *            Hashtable containing all columns with keys as the column names
	 *            and values as the object value for each column.
	 */
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, ClassNotFoundException {
		/*
		 * metaData is the previously stored information about all the created
		 * tables.
		 */
		HashMap<String, Couple[]> metaData = readMetaData();
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
		} else {
			FileInputStream fis = new FileInputStream("classes/" + strTableName + ".class");
			/*
			 * Following if() will be executed if the page contains records.
			 */
			if (fis.available() > 0) {
				ObjectInputStream ois = new ObjectInputStream(fis);
				Page readPage = (Page) (ois.readObject());
				HashMap<Object, Couple[]> table = readPage.getPage();
				ois.close();
				Object[] pkeys = table.keySet().toArray();
				/*
				 * Assuring each record has a unique key in the entire table.
				 */
				for (int i = 0; i < pkeys.length; i++) {
					if (htblColNameValue.containsValue(pkeys[i])) {
						System.out.println("From Database: Record with a similar id exists.");
						return;
					}
				}
			}
			/*
			 * Retrieving previously stored Hashtables for tables' files and
			 * pages and re-assigning class's instance variables with them.
			 */
			Hashtable<String, LinkedList<File>> tempFiles = readPageFiles(strTableName);
			if (tempFiles != null) {
				files = tempFiles;
			}
			Hashtable<String, LinkedList<Page>> tempPages = readPagePages(strTableName);
			if (tempPages != null) {
				pages = tempPages;
			}
			/*
			 * Table's keys and types retrieved for insertion checks.
			 */
			Couple[] tableData = metaData.get(strTableName);
			/*
			 * Array of couples to be passed and written on the disk.
			 */
			Couple[] rowData = new Couple[tableData.length - 1];
			LinkedList<Page> p = pages.get(strTableName);
			LinkedList<File> f = files.get(strTableName);
			Page tempPage = pages.get(strTableName).getLast();
			File tempFile = files.get(strTableName).getLast();
			Object[] keys = htblColNameValue.keySet().toArray();
			Object value;
			/*
			 * Assuring each column in the record a valid type according to the
			 * metadata.class.
			 */
			for (int i = 0; i < keys.length; i++) {
				value = htblColNameValue.get(keys[i]);
				Couple tempCoup = null;
				for (int j = 0; j < tableData.length; j++) {
					if (keys[i].equals(tableData[j].getKey())) {
						tempCoup = tableData[j];
						break;
					}
				}
				if (!value.getClass().toString().substring(6).equals(tempCoup.getValue().toString())) {
					System.out.println("From Database: Attempt to insert a record with incorrect type.");
					return;
				}
				/*
				 * After each Couple<Key, Restricted> and Couple<Key, Value>
				 * check, assigning Couple<Key, Value> to the rowData to be
				 * inserted after checks completion.
				 */
				rowData[i] = new Couple();
				rowData[i].setKey((String) keys[i]);
				rowData[i].setValue(value);
			}
			if (tempPage.full()) {
				/*
				 * New Page created and added at the end of the LinkedList
				 * concerning the table.
				 */
				tempPage = new Page();
				p.add(tempPage);
				/*
				 * New file added at the end of the LinkedList concerning the
				 * table.
				 */
				tempFile = new File("classes/" + strTableName + f.size() + ".class");
				f.add(tempFile);
			}
			/*
			 * Additional column added for the exact time when the record was
			 * inserted in the database.
			 */
			rowData[rowData.length - 1] = new Couple();
			rowData[rowData.length - 1].setKey("TouchDate");
			rowData[rowData.length - 1].setValue(Instant.now());
			System.out.println("From Database: Record inserted.");
			Couple[] rowInfo = metaData.get(strTableName);
			Object primaryKey = null;

			for (int i = 0; i < rowData.length; i++) { // getting primary key
				// value
				if (rowData[i].getKey().equals(rowInfo[0].getKey())) {
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
			ObjectOutputStream oos1 = new ObjectOutputStream(
					new FileOutputStream("classes/" + strTableName + "files.class"));
			oos1.writeObject(files); // Recording table's associated files info
			oos1.close();
			ObjectOutputStream oos11 = new ObjectOutputStream(
					new FileOutputStream("classes/" + strTableName + "pages.class"));
			oos11.writeObject(pages); // recording all pages of this table
			oos11.close();
		}

	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {
		boolean la2eto = false;
		HashMap<String, Couple[]> metaData = readMetaData();
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
		} else {

			/*
			 * Following if() will be executed if the page contains records.
			 */
			int index = 0;
			files = readPageFiles(strTableName);
			LinkedList<File> tableFiles = files.get(strTableName);
			for (int i = 0; i < tableFiles.size(); i++) {
				File tableFile = tableFiles.get(i);
				FileInputStream fis = new FileInputStream(tableFile);

				if (fis.available() > 0) {
					ObjectInputStream ois = new ObjectInputStream(fis);
					Page readPage = (Page) (ois.readObject());
					ois.close();
					HashMap<Object, Couple[]> table = readPage.getPage();
					Object[] arrayString = table.keySet().toArray();
					for (int j = 0; j < arrayString.length; j++) {
						la2eto = (arrayString[j] + "").equals(strKey + "");
						if (la2eto) {
							index = j;
							break;
						}
					}
					Couple[] get = null;
					Couple[] remove = null;
					System.out.println((table.keySet().toArray()[0] + "").equals(strKey));
					if (la2eto) {
						get = table.get(arrayString[index]);
						remove = table.remove(arrayString[index]);
						insertIntoTable(strTableName, htblColNameValue);
						System.out.println("Table updated");
						return;
					}
				}
			}
			System.out.println("Record not found.");
		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {
		boolean la2eto = false;
		Object[] keys = htblColNameValue.keySet().toArray();
		Couple[] rowData = new Couple[keys.length];
		for (int i = 0; i < keys.length; i++) {
			Object value = htblColNameValue.get(keys[i]);
			rowData[i] = new Couple();
			rowData[i].setKey((String) keys[i]);
			rowData[i].setValue(value);
		}
		HashMap<String, Couple[]> metaData = readMetaData();
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
		} else {

			/*
			 * Following if() will be executed if the page contains records.
			 */
			int index = 0;
			files = readPageFiles(strTableName);
			LinkedList<File> tableFiles = files.get(strTableName);
			for (int i = 0; i < tableFiles.size(); i++) {
				File tableFile = tableFiles.get(i);
				FileInputStream fis = new FileInputStream(tableFile);

				if (fis.available() > 0) {
					ObjectInputStream ois = new ObjectInputStream(fis);
					Page readPage = (Page) (ois.readObject());
					ois.close();
					HashMap<Object, Couple[]> table = readPage.getPage();
					Collection<Couple[]> arrayString = table.values();
					for (Couple[] couples : arrayString) {
						if (recordFound(couples, rowData)) {
							la2eto = true;
							System.out.println(table.containsValue(rowData));
							Couple[] remove = table.remove(couples);
							rowData = couples;
							break;
						}
					}
					return;
				}
				// System.out.println((table.keySet().toArray()[0] +
				// "").equals(strKey));
				// if (la2eto) {
				// get = table.get(arrayString[index]);
				// remove = table.remove(arrayString[index]);
				// return;
				// }
			}
		}
		System.out.println("Record not found.");
	}

	public boolean recordFound(Couple[] co, Couple[] ca) {
		for (int i = 0; i < ca.length; i++) {
			if (!(co[i].getValue().equals(ca[i].getValue()) && co[i].getKey().equals(ca[i].getKey())))
				return false;
		}
		return true;
	}

	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) {
		return null;
	}

}