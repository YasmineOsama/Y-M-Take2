package Potatoz;

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
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;

public class DBApp {
	static StringBuilder sb;
	static Hashtable<String, LinkedList<File>> files;
	static Hashtable<String, LinkedList<Page>> pages;
	static Hashtable<String, LinkedList<IndexCouple>> indices;
	static Hashtable<String, Hashtable<String, LinkedList<DensePage>>> dense;
	static Hashtable<String, Hashtable<String, LinkedList<RecordReference>>> denseBlock;
	static Hashtable<String, Hashtable<String, LinkedList<BRINPage>>> secIndices;
	int max;
	int maxBRIN;
	LinkedList<Couple[]> unlocatedData;

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
		prop.setProperty("MaximumRowsCountinPage", "2");
		prop.setProperty("BRINSize", "15");
		prop.store(new FileOutputStream("config/DBApp.config"),
				"MaximumRowsCountinPage as the name indicates specifies the maximum number of rows in a page."
						+ " BRINSize specifies the count of values that could be stored in a single BRIN file.");
		/*
		 * Config file created.
		 */
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("MaximumRowsCountinPage"));
		maxBRIN = Integer.parseInt(maxPage.getProperty("BRINSize"));
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
		indices = new Hashtable<String, LinkedList<IndexCouple>>();
		dense = new Hashtable<String, Hashtable<String, LinkedList<DensePage>>>();
		secIndices = new Hashtable<String, Hashtable<String, LinkedList<BRINPage>>>();
		denseBlock = new Hashtable<String, Hashtable<String, LinkedList<RecordReference>>>();
		storeInstances();
		/*
		 * Hashtables keeping track for every file and page created for all
		 * tables.
		 */
	}

	public void storeInstances() throws FileNotFoundException, IOException {
		ObjectOutputStream oos1 = new ObjectOutputStream(new FileOutputStream("classes/files.class"));
		oos1.writeObject(files); // Recording table's associated files info
		oos1.close();
		ObjectOutputStream oos11 = new ObjectOutputStream(new FileOutputStream("classes/pages.class"));
		oos11.writeObject(pages); // recording all pages of this table
		oos11.close();
		ObjectOutputStream oos111 = new ObjectOutputStream(new FileOutputStream("classes/indices.class"));
		oos111.writeObject(indices); // recording all pages of this table
		oos111.close();
		ObjectOutputStream oos1111 = new ObjectOutputStream(new FileOutputStream("classes/second_indices.class"));
		oos1111.writeObject(secIndices); // recording all pages of this
		// table
		oos1111.close();
		ObjectOutputStream oos11111 = new ObjectOutputStream(new FileOutputStream("classes/dense_indices.class"));
		oos11111.writeObject(dense); // recording all pages of this
		// table
		oos11111.close();
		ObjectOutputStream oos111111 = new ObjectOutputStream(
				new FileOutputStream("classes/dense_indices_block.class"));
		oos111111.writeObject(denseBlock); // recording all pages of this
		// table
		oos111111.close();
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

			LinkedList<File> tempfileList = new LinkedList<File>();
			/*
			 * LinkedList keeping track for all .class files for this table.
			 */
			LinkedList<Page> temppageList = new LinkedList<Page>();
			/*
			 * LinkedList keeping track for all pages created for this table.
			 */
			LinkedList<IndexCouple> tempindexList = new LinkedList<IndexCouple>();
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
			Hashtable<String, LinkedList<DensePage>> tempSecInd = new Hashtable<String, LinkedList<DensePage>>();
			File t = new File("classes/" + strTableName + "_data.class");
			t.createNewFile();
			tempfileList.add(t); // File added in the list of files for the
			// table.
			Page tempPage = new Page();
			temppageList.add(tempPage); // Page added in the list of pages for
			// the table.
			IndexCouple index = new IndexCouple(0, null);
			tempindexList.add(index);
			/*
			 * Instance variables for the DataBase updated with the trackers for
			 * files <LinkedList> and pages <LinkedList> concerning this
			 * specific table in the (files <Hashtable>) and (pages <Hashtable>)
			 * variables.
			 */
			files.put(strTableName, tempfileList);
			pages.put(strTableName, temppageList);
			indices.put(strTableName, tempindexList);
			dense.put(strTableName, tempSecInd);
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

			storeInstances();
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

	/***
	 * Method called after every insert.
	 * 
	 * @param strTableName:
	 *            Table name for which a BRINIndex will be created.
	 * @param strColName:
	 *            Column name on which a BRINIndex will be created.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void createBRINIndex(String strTableName, String strColName) throws ClassNotFoundException, IOException {
		/*
		 * metaData is the previously stored information about all the created
		 * tables.
		 */
		HashMap<String, Couple[]> metaData = readMetaData();
		/*
		 * Table's keys and types retrieved for insertion checks.
		 */
		Object type = null;
		Couple[] tableData = metaData.get(strTableName);
		if (tableData != null)
			for (Couple couple : tableData) {
				if (strColName.equals(couple.getKey())) {
					type = couple.getValue();
					break;
				}
			}
		if (true || !strColName.equals(findPrimaryColumn(tableData, strTableName))) {
			// create a file for indexing
			// sort data
			// store references for elements in hashtable
			Hashtable<String, LinkedList<BRINPage>> tempMainSecInd = readTableSecInd(strTableName);
			// createDensePages(strTableName, strColName, type);
			if (tempMainSecInd == null)
				tempMainSecInd = new Hashtable<String, LinkedList<BRINPage>>();
			else if (tempMainSecInd.contains(strColName))
				return;
			LinkedList<BRINPage> tempSecInd = sortWithRef(createDensePages(strTableName, strColName, type), strColName,
					strTableName);
			tempMainSecInd.put(strColName, tempSecInd);
			storeInstances();
		}
	}

	private LinkedList<DensePage> createDensePages(String strTableName, String strColName, Object t)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		Hashtable<String, LinkedList<DensePage>> tempMainDenseInd = readTableDense(strTableName);
		LinkedList<File> tempFiles = readTableFiles(strTableName);
		LinkedList<RecordReference> tempToSort = new LinkedList<RecordReference>();
		for (int fi = 0; fi < tempFiles.size(); fi++) {
			FileInputStream fis;
			if (fi == 0)
				fis = new FileInputStream("classes/" + strTableName + "_data.class");
			else
				fis = new FileInputStream("classes/" + strTableName + "_data_" + fi + ".class");
			/*
			 * Following if() will be executed if the page contains records.
			 */
			if (fis.available() > 0) {
				ObjectInputStream ois = new ObjectInputStream(fis);
				Page readTable = (Page) (ois.readObject());
				LinkedHashMap<Object, Couple[]> table = readTable.getPage();
				ois.close();
				Object[] pkeys = table.keySet().toArray();
				/*
				 * Assuring each record has a unique key in the entire table.
				 */
				int type = 0;
				findPrimaryColumn(null, strTableName);
				for (int i = 0; i < pkeys.length; i++) {
					Couple[] temp = table.get(pkeys[i]);
					for (int k = 0; k < temp.length; k++) {
						if (strColName.equals(temp[k].getKey())) {
							type = k;
							break;
						}
					}
				}

				for (int i = 0; i < pkeys.length; i++) {
					tempToSort.add(new RecordReference(findPrimaryKey(table.get(pkeys[i]), strTableName),
							table.get(pkeys[i])[type].getValue(), fi, t.toString().substring(10)));
				}
			}
			fis.close();
		}
		Hashtable<String, LinkedList<RecordReference>> denseBlock = readTableDenseBlock(strTableName);

		Collections.sort(tempToSort);
		if (denseBlock.get(strColName) == null)
			denseBlock.put(strColName, tempToSort);
		LinkedList<DensePage> densePages = new LinkedList<DensePage>();
		for (int i = 0; i < tempToSort.size(); i++) {
			if (i % max == 0)
				densePages.add(new DensePage(strTableName, strColName));
			densePages.getLast().add(tempToSort.get(i));
		}
		tempMainDenseInd.put(strColName, densePages);
		storeInstances();
		return densePages;
	}

	public LinkedList<BRINPage> sortWithRef(LinkedList<DensePage> records, String col, String tbl)
			throws ClassNotFoundException, IOException {
		LinkedList<BRINPage> tempBRIN = new LinkedList<BRINPage>();
		for (int i = 0; i < records.size(); i++) {
			RecordReference[] densePage = records.get(i).getDensePage();
			if (i % maxBRIN == 0)
				tempBRIN.add(new BRINPage(col, tbl));
			for (int j = 0; j < densePage.length; j++) {
				RecordReference recordReference = densePage[j];
				if (j % maxBRIN == 0) {

					tempBRIN.getLast().add(new IndexCoupleReference(recordReference.getContent(), null, i));
				} else if (recordReference != null)
					tempBRIN.getLast().getLast().setLast(recordReference.getContent());

			}

		}
		return tempBRIN;

	}

	/**
	 * Checker for previously similar created tables in the metadata.class.
	 * 
	 * @param strTableName:
	 *            Table name received by the method which has access to the disk
	 *            and re-reads the metadata to check if a table with similar
	 *            name is created.
	 */
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

	/**
	 * Method for getting all files contributing in all the tables' data, null
	 * if there's none.
	 * 
	 * @param strTableName:
	 *            Table name received by the method which has access to the disk
	 *            and return the stored written-on files for this table.
	 */
	public LinkedList<File> readTableFiles(String strTableName) throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/files.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, LinkedList<File>> tempFiles = (Hashtable<String, LinkedList<File>>) (ois.readObject());
			if (tempFiles != null)
				files = tempFiles;
			LinkedList<File> tempVar = tempFiles.get(strTableName);
			if (tempVar == null) {
				tempVar = new LinkedList<File>();
				tempFiles.put(strTableName, tempVar);
			}
			ois.close();
			return tempVar;
		}
		fis.close();
		return null;
	}

	public Hashtable<String, LinkedList<DensePage>> readTableDense(String strTableName)
			throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/dense_indices.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, Hashtable<String, LinkedList<DensePage>>> tempDense = (Hashtable<String, Hashtable<String, LinkedList<DensePage>>>) (ois
					.readObject());
			if (tempDense != null)
				dense = tempDense;
			Hashtable<String, LinkedList<DensePage>> tempVar = tempDense.get(strTableName);
			if (tempVar == null) {
				tempVar = new Hashtable<String, LinkedList<DensePage>>();
				tempDense.put(strTableName, tempVar);
			}
			ois.close();
			return tempVar;
		}

		fis.close();
		return null;
	}

	public Hashtable<String, LinkedList<RecordReference>> readTableDenseBlock(String strTableName)
			throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/dense_indices_block.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, Hashtable<String, LinkedList<RecordReference>>> tempDenseBlock = (Hashtable<String, Hashtable<String, LinkedList<RecordReference>>>) (ois
					.readObject());
			if (tempDenseBlock != null)
				denseBlock = tempDenseBlock;
			Hashtable<String, LinkedList<RecordReference>> tempVar = tempDenseBlock.get(strTableName);
			if (tempVar == null) {
				tempVar = new Hashtable<String, LinkedList<RecordReference>>();
				tempDenseBlock.put(strTableName, tempVar);
			}
			ois.close();
			return tempVar;
		}

		fis.close();
		return null;
	}

	public Hashtable<String, LinkedList<BRINPage>> readTableSecInd(String strTableName)
			throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/second_indices.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, Hashtable<String, LinkedList<BRINPage>>> tempSec = (Hashtable<String, Hashtable<String, LinkedList<BRINPage>>>) (ois
					.readObject());
			if (tempSec != null)
				secIndices = tempSec;
			Hashtable<String, LinkedList<BRINPage>> tempVar = tempSec.get(strTableName);
			if (tempVar == null) {
				tempVar = new Hashtable<String, LinkedList<BRINPage>>();
				tempSec.put(strTableName, tempVar);
			}
			ois.close();
			return tempVar;
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
	public LinkedList<Page> readTablePages(String strTableName) throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/pages.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, LinkedList<Page>> tempPages = (Hashtable<String, LinkedList<Page>>) (ois.readObject());
			if (tempPages != null)
				pages = tempPages;
			LinkedList<Page> tempVar = tempPages.get(strTableName);
			if (tempVar == null) {
				tempVar = new LinkedList<Page>();
				tempPages.put(strTableName, tempVar);
			}
			ois.close();
			return tempVar;
		}
		fis.close();
		return null;
	}

	/**
	 * Method for getting all indices contributing in all the tables' data, null
	 * if there's none.
	 * 
	 * @param strTableName:
	 *            Table name received by the method which has access to the disk
	 *            and return the stored written-on pages for this table.
	 */
	public LinkedList<IndexCouple> readTableIndices(String strTableName) throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream("classes/indices.class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			@SuppressWarnings("unchecked")
			Hashtable<String, LinkedList<IndexCouple>> tempIndices = (Hashtable<String, LinkedList<IndexCouple>>) (ois
					.readObject());
			if (tempIndices != null)
				indices = tempIndices;
			LinkedList<IndexCouple> tempVar = tempIndices.get(strTableName);
			if (tempVar == null) {
				tempVar = new LinkedList<IndexCouple>();
				tempIndices.put(strTableName, tempVar);
			}
			ois.close();
			return tempVar;
		}
		fis.close();
		return null;
	}

	public void insertNext(String strTableName, HashMap<String, Couple[]> metaData, Couple[] rowData, int counter,
			LinkedList<Page> p, LinkedList<File> f, LinkedList<IndexCouple> ind, Object number)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		int insertedIn = 0;
		for (; counter < p.size(); counter++) {

			Page tempPage = p.get(counter);
			File tempFile = f.get(counter);
			IndexCouple tempIndex = ind.get(counter);
			if (tempIndex.inRange((Integer) number, counter)) {
				// && tempPage.full() > 0
				/*
				 * Additional column added for the exact time when the record
				 * was inserted in the database.
				 */
				tempIndex.setFirst((Integer) number);
				// if (tempPage.full() <= 1)
				// tempIndex.setLast((Integer) number);
				System.out.println("From Database: Record inserted.");
				Couple[] rowInfo = metaData.get(strTableName);
				Object primaryKey = null;

				for (int i = 0; i < rowData.length; i++) { // getting
					// primary
					// key
					// value
					if (rowData[i].getKey().equals(rowInfo[0].getKey())) {
						primaryKey = rowData[i].getValue();
						break;
					}
				}
				// pages.put(strTableName, p);
				// files.put(strTableName, f);
				insertedIn = counter;
				Couple[] tobeAdded = tempPage.add(primaryKey, rowData); // Record
				// added
				// to
				// the
				// page.
				ObjectOutputStream oos = new ObjectOutputStream(
						new FileOutputStream(files.get(strTableName).getLast()));
				oos.writeObject(tempPage); // Record saved in the .class
				// file.
				oos.close();

				updateBRIN(strTableName, findPrimaryKey(rowData, strTableName), insertedIn);
				if (tobeAdded != null) {
					insertNext(strTableName, metaData, tobeAdded, counter, p, f, ind, number);
				}

				storeInstances();
				return;
			}
		}
		/*
		 * New Page created and added at the end of the LinkedList concerning
		 * the table.
		 */
		Page tempPage = new Page();
		File tempFile = new File("classes/" + strTableName + "_data_" + f.size() + ".class");
		IndexCouple tempIndex = new IndexCouple(0, null);
		tempIndex.setFirst((Integer) number);
		p.add(tempPage);
		/*
		 * New file added at the end of the LinkedList concerning the table.
		 */
		tempFile.createNewFile();
		f.add(tempFile);
		ind.add(tempIndex);
		rowData[rowData.length - 1] = new Couple();
		rowData[rowData.length - 1].setKey("TouchDate");
		rowData[rowData.length - 1].setValue(Instant.now());
		System.out.println("From Database: Record inserted.");
		Couple[] rowInfo = metaData.get(strTableName);
		Object primaryKey = null;

		for (int i = 0; i < rowData.length; i++) { // getting
			// primary
			// key
			// value
			if (rowData[i].getKey().equals(rowInfo[0].getKey())) {
				primaryKey = rowData[i].getValue();
				break;
			}
		}
		// pages.put(strTableName, p);
		// files.put(strTableName, f);
		insertedIn = counter;
		Couple[] tobeAdded = tempPage.add(primaryKey, rowData); // Record
		// added
		// to
		// the
		// page.
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(files.get(strTableName).getLast()));
		oos.writeObject(tempPage); // Record saved in the .class
		// file.
		oos.close();
		updateBRIN(strTableName, findPrimaryKey(rowData, strTableName), insertedIn);
		storeInstances();
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
		int insertedIn = 0;
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
		} else {
			/*
			 * Retrieving previously stored Hashtables for tables' files and
			 * pages and re-assigning class's instance variables with them.
			 */
			LinkedList<File> f = readTableFiles(strTableName);
			LinkedList<Page> p = readTablePages(strTableName);
			LinkedList<IndexCouple> ind = readTableIndices(strTableName);
			for (int fi = 0; fi < f.size(); fi++) {
				FileInputStream fis;
				if (fi == 0)
					fis = new FileInputStream("classes/" + strTableName + "_data.class");
				else
					fis = new FileInputStream("classes/" + strTableName + "_data_" + fi + ".class");
				/*
				 * Following if() will be executed if the page contains records.
				 */
				if (fis.available() > 0) {
					ObjectInputStream ois = new ObjectInputStream(fis);
					Page readTable = (Page) (ois.readObject());
					LinkedHashMap<Object, Couple[]> table = readTable.getPage();
					ois.close();
					Object[] pkeys = table.keySet().toArray();
					/*
					 * Assuring each record has a unique key in the entire
					 * table.
					 */
					for (int i = 0; i < pkeys.length; i++) {
						if (htblColNameValue.containsValue(pkeys[i])) {
							System.out.println("From Database: Record with a similar id exists.");
							return;
						}
					}
				}
				fis.close();
			}

			/*
			 * Table's keys and types retrieved for insertion checks.
			 */
			Couple[] tableData = metaData.get(strTableName);
			/*
			 * Array of couples to be passed and written on the disk.
			 */
			Couple[] rowData = new Couple[tableData.length - 1];

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

			Object number = findPrimaryKey(rowData, strTableName);
			for (int counter = 0; counter < p.size(); counter++) {
				Page tempPage = p.get(counter);
				File tempFile = f.get(counter);
				IndexCouple tempIndex = ind.get(counter);
				if (tempPage.full() > 0 || tempIndex.inRange((Integer) number, counter)) {
					/*
					 * Additional column added for the exact time when the
					 * record was inserted in the database.
					 */
					rowData[rowData.length - 1] = new Couple();
					rowData[rowData.length - 1].setKey("TouchDate");
					rowData[rowData.length - 1].setValue(Instant.now());
					System.out.println("From Database: Record inserted.");
					Couple[] rowInfo = metaData.get(strTableName);
					Object primaryKey = null;

					for (int i = 0; i < rowData.length; i++) { // getting
						// primary
						// key
						// value
						if (rowData[i].getKey().equals(rowInfo[0].getKey())) {
							primaryKey = rowData[i].getValue();
							break;
						}
					}
					// pages.put(strTableName, p);
					// files.put(strTableName, f);
					insertedIn = counter;
					Couple[] tobeAdded = tempPage.add(primaryKey, rowData); // Record
					// added
					// to
					// the
					// page.
					if (tempPage.full() == tempPage.max - 1 || (Integer) tempIndex.getFirst() >= (Integer) number)
						tempIndex.setFirst((Integer) number);
					if (tempPage.full() <= 1) {
						tempIndex
								.setLast(
										(Integer) findPrimaryKey(
												tempPage.getPage()
														.get(tempPage.getPage().keySet()
																.toArray()[tempPage.getPage().size() - 1]),
												strTableName));
					}
					ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(files.get(strTableName).getLast()));
					oos.writeObject(tempPage); // Record saved in the .class
					// file.
					oos.close();
					updateBRIN(strTableName, rowData, insertedIn);
					if (tobeAdded != null) {
						// unlocatedData.add(tobeAdded);

						number = findPrimaryKey(tobeAdded, strTableName);
						insertNext(strTableName, metaData, tobeAdded, counter, p, f, ind, number);
					}

					storeInstances();
					return;
				}
			}
			/*
			 * New Page created and added at the end of the LinkedList
			 * concerning the table.
			 */
			Page tempPage = new Page();
			File tempFile = new File("classes/" + strTableName + f.size() + ".class");
			IndexCouple tempIndex = new IndexCouple(0, null);
			p.add(tempPage);
			/*
			 * New file added at the end of the LinkedList concerning the table.
			 */
			tempFile = new File("classes/" + strTableName + f.size() + ".class");
			f.add(tempFile);
			ind.add(tempIndex);
			rowData[rowData.length - 1] = new Couple();
			rowData[rowData.length - 1].setKey("TouchDate");
			rowData[rowData.length - 1].setValue(Instant.now());
			System.out.println("From Database: Record inserted.");
			Couple[] rowInfo = metaData.get(strTableName);
			Object primaryKey = null;

			for (int i = 0; i < rowData.length; i++) { // getting
				// primary
				// key
				// value
				if (rowData[i].getKey().equals(rowInfo[0].getKey())) {
					primaryKey = rowData[i].getValue();
					break;
				}
			}
			// pages.put(strTableName, p);
			// files.put(strTableName, f);
			Couple[] tobeAdded = tempPage.add(primaryKey, rowData); // Record
			// added
			// to
			// the
			// page.
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(files.get(strTableName).getLast()));
			oos.writeObject(tempPage); // Record saved in the .class
			// file.
			oos.close();
			storeInstances();
		}
	}

	public void updateBRIN(String strTableName, Couple[] rowData, int insertedIn)
			throws ClassNotFoundException, IOException {
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
		} else {
			/*
			 * Retrieving previously stored Hashtables for tables' files and
			 * pages and re-assigning class's instance variables with them.
			 */
			HashMap<String, Couple[]> metaData = readMetaData();
			Couple[] temp = metaData.get(strTableName);
			Hashtable<String, LinkedList<BRINPage>> secInd = readTableSecInd(strTableName);
			Object[] tableCols = (Object[]) secInd.keySet().toArray();
			Hashtable<String, LinkedList<RecordReference>> indBlock = readTableDenseBlock(strTableName);
			for (int i = 0; i < tableCols.length; i++) {
				String string = (String) tableCols[i];
				for (int j = 0; j < rowData.length; j++) {
					String type = null;
					if (rowData[j].getKey().equals(string) && indBlock.get(string) != null) {
						for (int j2 = 0; j2 < temp.length; j2++) {
							if ((temp[j2].getKey().toString()).equals(string + "")) {
								type = temp[j2].getValue().toString().substring(10);
								break;
							}
						}
						indBlock.get(string).add(new RecordReference(findPrimaryKey(rowData, strTableName),
								rowData[j].getValue(), insertedIn, type));
					}
				}
				Collections.sort(indBlock.get(string));
				LinkedList<DensePage> densePages = new LinkedList<DensePage>();
				for (int i1 = 0; i1 < indBlock.get(string).size(); i1++) {
					if (i1 % max == 0)
						densePages.add(new DensePage(strTableName, string));
					densePages.getLast().add(indBlock.get(string).get(i1));
				}
				LinkedList<BRINPage> tempSecInd = sortWithRef(densePages, string, strTableName);
				Hashtable<String, LinkedList<BRINPage>> tempMainSecInd = readTableSecInd(strTableName);
				tempMainSecInd.remove(string);
				tempMainSecInd.put(string, tempSecInd);
				Hashtable<String, LinkedList<DensePage>> tempMainDenseInd = readTableDense(strTableName);
				tempMainDenseInd.remove(string);
				tempMainDenseInd.put(string, densePages);
				storeInstances();
			}

		}

	}

	public void updateBRIN(String strTableName, Object key, int in) throws ClassNotFoundException, IOException {
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
		} else {
			/*
			 * Retrieving previously stored Hashtables for tables' files and
			 * pages and re-assigning class's instance variables with them.
			 */
			HashMap<String, Couple[]> metaData = readMetaData();
			Couple[] temp = metaData.get(strTableName);
			Hashtable<String, LinkedList<BRINPage>> secInd = readTableSecInd(strTableName);
			Object[] tableCols = (Object[]) secInd.keySet().toArray();
			Hashtable<String, LinkedList<RecordReference>> indBlock = readTableDenseBlock(strTableName);
			for (int i = 0; i < tableCols.length; i++) {
				String string = (String) tableCols[i];
				Collections.sort(indBlock.get(string));
				LinkedList<DensePage> densePages = new LinkedList<DensePage>();
				for (int i1 = 0; i1 < indBlock.get(string).size(); i1++) {
					if (indBlock.get(string).get(i1).getId().equals(key))
						indBlock.get(string).get(i1).setLocation(in);
					if (i1 % max == 0)
						densePages.add(new DensePage(strTableName, string));
					densePages.getLast().add(indBlock.get(string).get(i1));
				}
				LinkedList<BRINPage> tempSecInd = sortWithRef(densePages, string, strTableName);
				Hashtable<String, LinkedList<BRINPage>> tempMainSecInd = readTableSecInd(strTableName);
				tempMainSecInd.remove(string);
				tempMainSecInd.put(string, tempSecInd);
				Hashtable<String, LinkedList<DensePage>> tempMainDenseInd = readTableDense(strTableName);
				tempMainDenseInd.remove(string);
				tempMainDenseInd.put(string, densePages);
				storeInstances();
			}

		}

	}

	public void updateBRIN(String strTableName) throws ClassNotFoundException, IOException {
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
		} else {
			/*
			 * Retrieving previously stored Hashtables for tables' files and
			 * pages and re-assigning class's instance variables with them.
			 */
			HashMap<String, Couple[]> metaData = readMetaData();
			Couple[] temp = metaData.get(strTableName);
			Hashtable<String, LinkedList<BRINPage>> secInd = readTableSecInd(strTableName);
			Object[] tableCols = (Object[]) secInd.keySet().toArray();
			Hashtable<String, LinkedList<RecordReference>> indBlock = readTableDenseBlock(strTableName);
			for (int i = 0; i < tableCols.length; i++) {
				String string = (String) tableCols[i];
				Collections.sort(indBlock.get(string));
				LinkedList<DensePage> densePages = new LinkedList<DensePage>();
				for (int i1 = 0; i1 < indBlock.get(string).size(); i1++) {
					if (i1 % max == 0)
						densePages.add(new DensePage(strTableName, string));
					densePages.getLast().add(indBlock.get(string).get(i1));
				}
				LinkedList<BRINPage> tempSecInd = sortWithRef(densePages, string, strTableName);
				Hashtable<String, LinkedList<BRINPage>> tempMainSecInd = readTableSecInd(strTableName);
				tempMainSecInd.remove(string);
				tempMainSecInd.put(string, tempSecInd);
				Hashtable<String, LinkedList<DensePage>> tempMainDenseInd = readTableDense(strTableName);
				tempMainDenseInd.remove(string);
				tempMainDenseInd.put(string, densePages);
				storeInstances();
			}

		}

	}

	/***
	 * On an existing record update.
	 * 
	 * @param strTableName:
	 *            Table name to be used for the update.
	 * @param strKey:
	 *            Key used to identify records to be updated.
	 * @param htblColNameValue:
	 *            Record after modification.
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {
		boolean found = false;
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist.");
		} else {

			/*
			 * Following if() will be executed if the page contains records.
			 */
			int index = 0;
			LinkedList<File> f = readTableFiles(strTableName);
			LinkedList<Page> p = readTablePages(strTableName);
			LinkedList<IndexCouple> ind = readTableIndices(strTableName);
			for (int fi = 0; fi < f.size(); fi++) {
				FileInputStream fis;
				String fileName;
				if (fi == 0) {
					fileName = "classes/" + strTableName + "_data.class";
					fis = new FileInputStream(fileName);
				} else {
					fileName = "classes/" + strTableName + "_data_" + fi + ".class";
					fis = new FileInputStream(fileName);
				}
				if (fis.available() > 0) {
					ObjectInputStream ois = new ObjectInputStream(fis);
					Page readTable = (Page) (ois.readObject());
					ois.close();
					LinkedHashMap<Object, Couple[]> table = readTable.getPage();
					Object[] arrayString = table.keySet().toArray();
					/*
					 * For loop to get the index of the data with the
					 * corresponding strKey.
					 */
					for (int j = 0; j < arrayString.length; j++) {
						found = (arrayString[j] + "").equals(strKey + "");
						if (found) {
							index = j;
							break;
						}
					}

					/*
					 * Found is a boolean for the updating procedure for the
					 * index.
					 */
					if (found) {
						Couple[] get = table.get(arrayString[index]);
						Hashtable<String, Object> updatedRow = newRow(get, htblColNameValue);
						table.remove(arrayString[index]);
						deleteRecordById(strTableName, arrayString[index], get);
						readTable.setPage(table);
						ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName));
						oos.writeObject(readTable); // Record saved in the
						// .class file.
						oos.close();
						storeInstances();
						insertIntoTable(strTableName, updatedRow);
						System.out.println("From Database: Table updated.");
						return;
					}
				}
			}
			System.out.println("Record not found.");
		}
	}

	/***
	 * Method responsible for updating the record.
	 * 
	 * @param oldRow:
	 *            Existing record passed to the method.
	 * @param updates:
	 *            Hashtable containing wanted modifications.
	 * @return
	 */
	public Hashtable<String, Object> newRow(Couple[] oldRow, Hashtable<String, Object> updates) {
		Object[] keys = updates.keySet().toArray();
		Couple[] rowData = new Couple[keys.length];
		for (int i = 0; i < keys.length; i++) {
			Object value = updates.get(keys[i]);
			rowData[i] = new Couple();
			rowData[i].setKey((String) keys[i]);
			rowData[i].setValue(value);
		}
		Hashtable<String, Object> updatedRow = new Hashtable<String, Object>();
		for (int i = 0; i < oldRow.length; i++) {
			boolean found = false;
			for (int j = 0; j < rowData.length; j++) {
				if (oldRow[i].getKey().equals(rowData[j].getKey())) {
					updatedRow.put(oldRow[i].getKey(), rowData[j].getValue());
					found = true;
					break;
				}
				if (!found && !((oldRow[i].getKey() + "").equals("TouchDate"))) {
					updatedRow.put(oldRow[i].getKey(), oldRow[i].getValue());
				}
			}
		}
		return updatedRow;
	}

	public Couple[] deleteRecordById(String strTableName, Object key, Couple[] c)
			throws ClassNotFoundException, IOException {
		LinkedList<File> tableFiles = readTableFiles(strTableName);
		LinkedList<IndexCouple> tempInd = readTableIndices(strTableName);
		/*
		 * For loop iterating over all written files on the disk to find the
		 * record and handling the deletion process.
		 */
		for (int i = 0; i < tableFiles.size(); i++) {
			if (tempInd.get(i) != null)
				if ((Integer) tempInd.get(i).getFirst() <= (Integer) key
						&& (tempInd.get(i).getLast() == null || (Integer) tempInd.get(i).getLast() >= (Integer) key)) {

					File tableFile = tableFiles.get(i);
					FileInputStream fis = new FileInputStream(tableFile);

					if (fis.available() > 0) {
						ObjectInputStream ois = new ObjectInputStream(fis);
						Page readTable = (Page) (ois.readObject());
						ois.close();
						LinkedHashMap<Object, Couple[]> table = readTable.getPage();
						Object[] arrayString = table.keySet().toArray();
						/*
						 * For loop removing the record once it is found.
						 */
						if (table.containsKey(key)) {
							table.remove(key);
							ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tableFile.getName()));
							oos.writeObject(readTable); // Record saved in the
							// .class file.
							oos.close();
							Hashtable<String, LinkedList<RecordReference>> tempDense = readTableDenseBlock(
									strTableName);
							Object[] tempKeys = tempDense.keySet().toArray();
							for (int j = 0; j < tempKeys.length; j++) {
								LinkedList<RecordReference> tempBlock = tempDense.get(tempKeys[j]);
								Object val = null;
								for (int l = 0; l < c.length; l++) {
									if (c[l].getKey().equals(tempKeys[j])) {
										val = c[l].getValue();
										break;
									}
								}
								for (RecordReference recordReference : tempBlock) {
									if (recordReference.equals(key))
										tempBlock.remove(val);
								}
							}
							storeInstances();
							updateBRIN(strTableName);

						}

					}
				}
		}
		return null;
	}

	/***
	 * On an existing record deletion.
	 * 
	 * @param strTableName:
	 *            Table used for deletion process.
	 * @param htblColNameValue:
	 *            Record to be deleted passed to the method as a Hashtable.
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {
		int recordsDeleted = 0;
		Object[] keys = htblColNameValue.keySet().toArray();
		Couple[] rowData = new Couple[keys.length];
		/*
		 * Following for loop for creating a Couple[] from the given
		 * htblColNameValue.
		 */
		for (int i = 0; i < keys.length; i++) {
			Object value = htblColNameValue.get(keys[i]);
			rowData[i] = new Couple();
			rowData[i].setKey((String) keys[i]);
			rowData[i].setValue(value);
		}
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist.");
		} else {

			/*
			 * Following if() will be executed if the page contains records.
			 */
			LinkedList<File> tableFiles = readTableFiles(strTableName);
			LinkedList<IndexCouple> tempInd = readTableIndices(strTableName);
			/*
			 * For loop iterating over all written files on the disk to find the
			 * record and handling the deletion process.
			 */
			for (int i = 0; i < tableFiles.size(); i++) {
				File tableFile = tableFiles.get(i);
				FileInputStream fis = new FileInputStream(tableFile);

				if (fis.available() > 0) {
					ObjectInputStream ois = new ObjectInputStream(fis);
					Page readTable = (Page) (ois.readObject());
					ois.close();
					LinkedHashMap<Object, Couple[]> table = readTable.getPage();
					Object[] arrayString = table.values().toArray();
					/*
					 * For loop removing the record once it is found.
					 */
					for (int j = 0; j < arrayString.length; j++) {
						if (recordFound((Couple[]) arrayString[j], rowData)) {
							Couple[] get = table.get(arrayString[j]);
							Object primaryKey = findPrimaryKey((Couple[]) arrayString[j], strTableName);
							table.remove(primaryKey);
							deleteRecordById(strTableName, primaryKey, get);
							recordsDeleted++;
						}
					}
					/*
					 * Object re-written on the disk after the deletion.
					 */
					readTable.setPage(table);
					ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tableFile.getName()));
					oos.writeObject(readTable); // Record saved in the
					// .class
					// file.
					oos.close();
					storeInstances();
				}
			}
		}
		System.out.println("From Database: " + recordsDeleted + " records deleted.");
	}

	/***
	 * Method used for finding the primary key.
	 * 
	 * @param rowData:
	 *            Record from a certain table.
	 * @param strTableName:
	 *            Table name used for finding the primary key for this table.
	 */
	private Object findPrimaryKey(Couple[] rowData, String strTableName) throws ClassNotFoundException, IOException {
		HashMap<String, Couple[]> metaData = readMetaData();
		Couple[] rowInfo = metaData.get(strTableName);
		Object primaryKey = null;
		for (int i = 0; i < rowData.length; i++) { // getting primary key value
			if (rowData[i].getKey().equals(rowInfo[0].getKey())) {
				primaryKey = rowData[i].getValue();
				break;
			}
		}
		return primaryKey;
	}

	/***
	 * Method used for finding the primary column.
	 * 
	 * @param rowData:
	 *            Record from a certain table.
	 * @param strTableName:
	 *            Table name used for finding the primary column for this table.
	 */
	private Object findPrimaryColumn(Couple[] rowData, String strTableName) throws ClassNotFoundException, IOException {
		HashMap<String, Couple[]> metaData = readMetaData();
		Couple[] rowInfo = metaData.get(strTableName);
		String primaryCol = null;
		// for (int i = 0; i < rowData.length; i++) { // getting primary key
		// value
		// if (rowData[i].getKey().equals(rowInfo[0].getKey())) {
		// primaryCol = rowData[i].getKey();
		// break;
		// }
		// }
		return rowInfo[0].getKey();
		// return primaryCol;
	}

	/***
	 * Method used for signaling with a boolean for two identical records.
	 * 
	 * @param arrayString:
	 *            Record from the original table.
	 * @param rowData:
	 *            Record as an input from the user.
	 * @return
	 */
	public boolean recordFound(Couple[] arrayString, Couple[] rowData) {
		int found = 0;
		for (int i = 0; i < rowData.length; i++) {
			for (int j = 0; j < arrayString.length; j++) {
				if (((arrayString[j]).getValue()).equals(rowData[i].getValue())
						&& ((arrayString[j]).getKey()).equals(rowData[i].getKey()))
					found++;
			}
			if (found == rowData.length) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) throws ClassNotFoundException, IOException {
		Collection<Object> result = new ArrayList<Object>();
		if (strarrOperators.length > 2 || strarrOperators.length < 1) {
			System.out.println("From Database: invalid number of operators");
		}
		for (int i = 0; i < strarrOperators.length; i++) {
			String op = strarrOperators[i];
			if (!(op.equals(">") || op.equals("<") || op.equals(">=") || op.equals("<="))) {
				System.out.println("From Database: Invalid Operator");
				return null;
			}
		}
		if (!(strarrOperators.length == objarrValues.length)) {
			System.out.println("From Database: Number of operators must equal number of values");
			return null;
		}
		HashMap<String, Couple[]> metaData = readMetaData();
		if (!tableExists(strTableName)) {
			System.out.println("From Database: Table doesn't exist");
			return null;
		} else {
			Object value = null;
			Couple[] tableData = metaData.get(strTableName);
			for (int i = 0; i < tableData.length; i++) {
				if (tableData[i].getKey() == strColumnName) {
					value = tableData[i].getValue();
					break;
				}
			}
			if (value == null) {
				System.out.println("From Database: Invalid column name");
				return null;
			}
			for (int i = 0; i < objarrValues.length; i++) {
				if (!objarrValues[i].getClass().toString().substring(6).equals(value.toString())) {
					System.out.println("From Database: value type is incompatible with column type");
					return null;
				}
			}
			String type = null;
			if (objarrValues[0].getClass().toString().substring(6).equals("java.lang.Integer")) {
				type = "int";
			}
			if (objarrValues[0].getClass().toString().substring(6).equals("java.lang.String")) {
				type = "str";
			}
			if (objarrValues[0].getClass().toString().substring(6).equals("java.lang.double")) {
				type = "dbl";
			}
			createBRINIndex(strTableName, strColumnName);
			if (strColumnName.equals(tableData[0])) {
				LinkedList<IndexCouple> index = readTableIndices(strTableName);
				for (int i = 0; i < objarrValues.length; i++) {
					String op = strarrOperators[i];
					if (i > 0) {
						switch (op) {
						case ">":
							op = "<";
							break;
						case "<":
							op = ">";
							break;
						case ">=":
							op = "<=";
							break;
						case "<=":
							op = "<=";
						}
					}
					for (int j = 0; j < index.size(); j++) {
						switch (type) {
						case "int": {
							int obvalue = (int) objarrValues[i];
							int first = (int) index.get(j).getFirst();
							int last = (int) index.get(j).getLast();
							switch (op) {
							case ">": {
								if (obvalue > first) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue <= (int) pkeys[k]) {
											break;
										}
										result.add(pkeys[k]);
									}
								}
								break;
							}
							case "<": {
								if (obvalue < last) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue < (int) pkeys[k]) {
											result.add(pkeys[k]);
										}
									}
								}
								break;
							}
							case ">=": {
								if (obvalue >= first) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue < (int) pkeys[k]) {
											break;
										}
										result.add(pkeys[k]);
									}
								}
								break;
							}
							case "<=": {
								if (obvalue <= last) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue <= (int) pkeys[k]) {
											result.add(pkeys[k]);
										}
									}
								}
							}
							}
							break;
						}
						case "str": {
							String obvalue = (String) objarrValues[i];
							String first = (String) index.get(j).getFirst();
							String last = (String) index.get(j).getLast();
							switch (op) {
							case ">": {
								if (obvalue.compareTo(first) > 0) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue.compareTo((String) pkeys[k]) <= 0) {
											break;
										}
										result.add(pkeys[k]);
									}
								}
								break;
							}
							case "<": {
								if (obvalue.compareTo(last) < 0) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue.compareTo((String) pkeys[k]) < 0) {
											result.add(pkeys[k]);
										}
									}
								}
								break;
							}
							case ">=": {
								if (obvalue.compareTo(first) >= 0) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue.compareTo((String) pkeys[k]) < 0) {
											break;
										}
										result.add(pkeys[k]);
									}
								}
								break;
							}
							case "<=": {
								if (obvalue.compareTo(last) <= 0) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue.compareTo((String) pkeys[k]) <= 0) {
											result.add(pkeys[k]);
										}
									}
								}
							}
							}
							break;

						}
						case "dbl":
							double obvalue = (double) objarrValues[i];
							double first = (double) index.get(j).getFirst();
							double last = (double) index.get(j).getLast();
							switch (op) {
							case ">": {
								if (obvalue > first) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue <= (double) pkeys[k]) {
											break;
										}
										result.add(pkeys[k]);
									}
								}
								break;
							}
							case "<": {
								if (obvalue < last) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue < (double) pkeys[k]) {
											result.add(pkeys[k]);
										}
									}
								}
								break;
							}
							case ">=": {
								if (obvalue >= first) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue < (double) pkeys[k]) {
											break;
										}
										result.add(pkeys[k]);
									}
								}
								break;
							}
							case "<=": {
								if (obvalue <= last) {
									LinkedHashMap<Object, Couple[]> table = table(j, strTableName);
									Object[] pkeys = table.keySet().toArray();
									for (int k = 0; k < pkeys.length; k++) {
										if (obvalue <= (double) pkeys[k]) {
											result.add(pkeys[k]);
										}
									}
								}
							}
							}
							break;
						}
					}
				}
			} else {
				Hashtable<String, LinkedList<BRINPage>> secInd = readTableSecInd(strTableName);
			}

		}

		return null;
	}

	public LinkedHashMap<Object, Couple[]> table(int page, String strTableName)
			throws IOException, ClassNotFoundException {
		FileInputStream fis;
		if (page == 0)
			fis = new FileInputStream("classes/" + strTableName + "_data.class");
		else
			fis = new FileInputStream("classes/" + strTableName + "_data_" + page + ".class");
		if (fis.available() > 0) {
			ObjectInputStream ois = new ObjectInputStream(fis);
			Page readTable = (Page) (ois.readObject());
			ois.close();
			return readTable.getPage();
		}
		fis.close();
		return null;

	}

}