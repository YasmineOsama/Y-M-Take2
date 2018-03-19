package Potatoz;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class BRINPage implements Serializable {
	private static final long serialVersionUID = 1L;
	IndexCoupleReference[] brinPage;
	int max;
	int count = 0;
	String colName;
	String tblName;

	public BRINPage(IndexCoupleReference[] refs, String col, String tbl) {
		brinPage = refs;
		colName = col;
		tblName = tbl;
	}

	public IndexCoupleReference[] getBRINPage() {
		return brinPage;
	}

	public void setBRINPage(IndexCoupleReference[] brinPage) {
		this.brinPage = brinPage;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public BRINPage(String col, String tbl) throws IOException {
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("BRINSize"));
		brinPage = new IndexCoupleReference[max];
		colName = col;
		tblName = tbl;
	}

	public void add(IndexCoupleReference r) {
		brinPage[count++] = r;
	}

	public IndexCoupleReference getLast() {
		int temp = count - 1;
		return brinPage[temp];
	}

	public int full() {
		return max - count;
	}
}
