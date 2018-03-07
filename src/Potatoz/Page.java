package Potatoz;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public class Page implements Serializable, Comparator<Map.Entry<String, Integer>> {
	private static final long serialVersionUID = 1L;
	public LinkedHashMap<Object, Couple[]> page;
	public int max;
	public static String tableName;

	public Page() throws IOException {
		page = new LinkedHashMap<Object, Couple[]>();
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("MaximumRowsCountinPage"));
	}

	public void add(Object pKey, Couple[] row) {
		page.put(pKey, row);
		// Collections.sor
//		Set<Entry<Object, Couple[]>> x = page.entrySet();
//		LinkedHashMap<Object, Couple[]> y = new LinkedHashMap<Object, Couple[]>();
//		Collections.sort((List<>) x, new Comparator() {
//			public int compare(Object o1, Object o2) {
//				if ((Integer) o1 < (Integer) o2)
//					return 1;
//				if ((Integer) o1 > (Integer) o2)
//					return -1;
//				return 0;
//			}
//		});

	}

	public LinkedHashMap<Object, Couple[]> getPage() {
		return page;
	}

	public void setPage(LinkedHashMap<Object, Couple[]> page) {
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

	public int compare(Entry<String, Integer> c0, Entry<String, Integer> c1) {

		return 0;
	}

}