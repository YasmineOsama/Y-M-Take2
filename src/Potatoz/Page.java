package Potatoz;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;

<<<<<<< HEAD
public class Page implements Serializable {
=======
public class Page implements Serializable{
>>>>>>> 2cc557634fb55d0afe186ca2c13c15b9882eca81

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public ArrayList<Hashtable<String, Object>> page;
	public int max;
	public static String tableName;
<<<<<<< HEAD

	public Page() throws IOException {
		page = new ArrayList<Hashtable<String, Object>>();
=======
	public Page() throws IOException {
		page = new ArrayList<Hashtable<String,Object>>();
>>>>>>> 2cc557634fb55d0afe186ca2c13c15b9882eca81
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("MaximumRowsCountinPage"));
<<<<<<< HEAD
		max = 1;
	}

	public void add(Hashtable<String, Object> row) {
		page.add(row);
	}

	public boolean full() {
		if (page.size() == max) {
=======

	}
	public void add(Hashtable<String, Object> row) {
		page.add(row);
	}
	public boolean full(ArrayList<Hashtable<String, Object>> page) {
		if(page.size() == max) {
>>>>>>> 2cc557634fb55d0afe186ca2c13c15b9882eca81
			return true;
		}
		return false;
	}

}
