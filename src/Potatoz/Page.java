package Potatoz;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;

public class Page implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public ArrayList<Hashtable<String, Object>> page;
	public int max;
	public static String tableName;

	public Page() throws IOException {
		page = new ArrayList<Hashtable<String, Object>>();
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("MaximumRowsCountinPage"));
		max = 1;
	}

	public void add(Hashtable<String, Object> row) {
		page.add(row);
	}

	public boolean full() {
		if (page.size() == max) {
			return true;
		}
		return false;
	}

}
