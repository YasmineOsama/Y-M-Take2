package Potatoz;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class BRINPage implements Serializable {
	private static final long serialVersionUID = 1L;
	IndexCouple[] brinPage;
	int max;
	int count = 0;
	String colName;
	public BRINPage(IndexCouple[] refs, String col){
		brinPage = refs;
		colName = col;
	}
	public IndexCouple[] getBRINPage() {
		return brinPage;
	}
	public void setBRINPage(IndexCouple[] brinPage) {
		this.brinPage = brinPage;
	}
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
	public BRINPage(String col) throws IOException{
		File config = new File("config/DBApp.config");
		FileReader read = new FileReader(config);
		Properties maxPage = new Properties();
		maxPage.load(read);
		max = Integer.parseInt(maxPage.getProperty("BRINSize"));
		brinPage = new IndexCouple[max];
		colName = col;
	}
	public void add(IndexCouple r){
		brinPage[count++] = r;
	}
	public int full(){
		return max - count;
	}
}
