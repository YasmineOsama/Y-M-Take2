package Potatoz;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Hashtable;

public class DBAppTest {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		DBApp app = new DBApp();
		app.init();
		System.out.println(
				"**This test will show incorrect results if tried more than once or if classes/ directory and data/ directory are not empty**");
		String strTableName = "Students";
		System.out.println("- Trying to create a table 'Students'.");
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		app.createTable(strTableName, "id", htblColNameType);
		System.out.println("- Trying to insert a record with id 8.");
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("id", new Integer(8));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.5));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with id 5674567.");
		htblColNameValue.put("id", new Integer(5674567));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Double(1.25));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with id 9.");
		htblColNameValue.put("id", new Integer(9));
		htblColNameValue.put("name", new String("Hany Mohamed"));
		htblColNameValue.put("gpa", new Double(1.0));
		app.insertIntoTable(strTableName, htblColNameValue);
		app.createBRINIndex("Students", "name");
		htblColNameValue.clear();
		System.out.println("- Trying to insert a duplicate record.");
		htblColNameValue.put("id", new Integer(5674567));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Double(1.25));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with incorrect type.");
		htblColNameValue.put("id", new Integer(999999));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Integer(1));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with id 23498.");
		htblColNameValue.put("id", new Integer(23498));
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with id 23498.");
		htblColNameValue.put("id", new Integer(23498));
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double(1.5));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with id 78452.");
		htblColNameValue.put("id", new Integer(78452));
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double(0.88));
		app.insertIntoTable(strTableName, htblColNameValue);
		FileInputStream fis = new FileInputStream("classes/Students.class");
		ObjectInputStream ois = new ObjectInputStream(fis);
		Page p = (Page) ois.readObject();
		HashMap<Object, Couple[]> metaData = p.getPage();
		ois.close();
		Couple[] b = metaData.get(8);
		System.out.println("Displaying a record from the stored table 'Students' read from the disk:");
		System.out.print(b[0].getKey() + ": ");
		System.out.println(b[0].getValue());
		System.out.print(b[1].getKey() + ": ");
		System.out.println(b[1].getValue());
		System.out.print(b[2].getKey() + ": ");
		System.out.println(b[2].getValue());
		System.out.print(b[3].getKey() + ": ");
		System.out.println(b[3].getValue());
		strTableName = "Teachers";
		System.out.println("- Trying to create a table 'Teachers'.");
		htblColNameType.clear();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		app.createTable(strTableName, "id", htblColNameType);
		System.out.println("- Trying to insert a record with id 8.");
		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(8));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.5));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with id 5674567.");
		htblColNameValue.put("id", new Integer(5674567));
		htblColNameValue.put("name", new String("Teacher Teacher ya3ni"));
		htblColNameValue.put("gpa", new Double(1.25));
		app.insertIntoTable(strTableName, htblColNameValue);
		htblColNameValue.clear();
		System.out.println("- Trying to insert a record with id 1234.");
		htblColNameValue.put("id", new Integer(1234));
		htblColNameValue.put("name", new String("yasoo"));
		htblColNameValue.put("gpa", new Double(1.25));
		app.insertIntoTable(strTableName, htblColNameValue);

		fis = new FileInputStream("classes/Teachers.class");
		ois = new ObjectInputStream(fis);
		p = (Page) ois.readObject();
		metaData = p.getPage();
		ois.close();
		b = metaData.get(5674567);
		System.out.println("Displaying a record from the stored table 'Teachers' read from the disk:");
		System.out.print(b[0].getKey() + ": ");
		System.out.println(b[0].getValue());
		System.out.print(b[1].getKey() + ": ");
		System.out.println(b[1].getValue());
		System.out.print(b[2].getKey() + ": ");
		System.out.println(b[2].getValue());
		System.out.print(b[3].getKey() + ": ");
		System.out.println(b[3].getValue());
		System.out.println("- Trying to update a record with the id of 5674567 with the following values.");
		htblColNameValue.clear();
		htblColNameValue.put("name", new String("yasoo"));
		htblColNameValue.put("gpa", new Double(1.25));
		app.updateTable(strTableName, "5674567", htblColNameValue);
		System.out.println("- Trying to delete records with two parameters.");
		htblColNameValue.clear();
		htblColNameValue.put("name", new String("yasoo"));
		htblColNameValue.put("gpa", new Double(0.25));
		app.deleteFromTable(strTableName, htblColNameValue);
		System.out.println("- Trying to delete records with one parameter.");
		htblColNameValue.clear();
		htblColNameValue.put("name", new String("yasoo"));
		app.deleteFromTable(strTableName, htblColNameValue);
	}
}