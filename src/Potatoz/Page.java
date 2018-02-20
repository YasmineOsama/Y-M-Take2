package Potatoz;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

public class Page implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public HashMap<Object, Couple[]> page;
	public int max;
	public static String tableName;

	public Page() throws IOException {
		page = new HashMap<Object, Couple[]>();
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("MaximumRowsCountinPage"));
		//max = 1;
	}

	public void add(Object pKey, Couple[] row) {
		page.put(pKey, row);
	}

	public HashMap<Object, Couple[]> getPage() {
		return page;
	}

	public void setPage(HashMap<Object, Couple[]> page) {
		this.page = page;
	}

	public static String getTableName() {
		return tableName;
	}

	public static void setTableName(String tableName) {
		Page.tableName = tableName;
	}

	public boolean full() {
		if (page.size() == max) {
			return true;
		}
		return false;
	}

}