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
	
	public DensePage(RecordReference[] refs, String col){
		densePage = refs;
		colName = col;
	}
	public DensePage(String col) throws IOException{
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("MaximumRowsCountinPage"));
		densePage = new RecordReference[max];
		colName = col;
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
	public void add(RecordReference r){
		densePage[count++] = r;
	}
	public int full(){
		return max - count;
	}
	

}
