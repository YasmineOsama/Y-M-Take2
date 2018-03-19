package Potatoz;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Properties;

public class DensePage implements Serializable {
	private static final long serialVersionUID = 1L;
	RecordReference[] densePage;
	int max;
	int count = 0;
	String colName;
	String tblName;

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public RecordReference getLast() {
		return densePage[count - 1];
	}

	public RecordReference getFirt() {
		return densePage[0];
	}

	public String getTblName() {
		return tblName;
	}

	public void setTblName(String tblName) {
		this.tblName = tblName;
	}

	public DensePage(RecordReference[] refs, String table, String col) {
		densePage = refs;
		colName = col;
		tblName = table;
	}

	public DensePage(String table, String col) throws IOException {
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("MaximumRowsCountinPage"));
		densePage = new RecordReference[max];
		colName = col;
		tblName = table;
	}

	public RecordReference[] getDensePage() {
		return densePage;
	}

	public void setDensePage(RecordReference[] densePage) {
		this.densePage = densePage;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public void add(RecordReference r) {
		densePage[count++] = r;
	}

	public int full() {
		return max - count;
	}

}
