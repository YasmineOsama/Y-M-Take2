package Potatoz;

import java.io.Serializable;

public class RecordReference implements Serializable, Comparable<Object> {
	private static final long serialVersionUID = 1L;
	Object id;
	Object content;
	int location;
	String type;

	public Object getId() {
		return id;
	}

	public void setId(Object id) {
		this.id = id;
	}

	public Object getContent() {
		return content;
	}

	public void setContent(Object content) {
		this.content = content;
	}

	public int getLocation() {
		return location;
	}

	public void setLocation(int location) {
		this.location = location;
	}

	public RecordReference(Object id, Object obj, int location, String type) {
		content = obj;
		this.id = id;
		this.location = location;
		this.type = type;
	}

	public int compareTo(Object o) {
		switch (type) {
		case "String":
			return ((String) getContent()).compareTo((String) ((RecordReference) o).getContent());
		case "Integer":
			return ((Integer) getContent()).compareTo((Integer) ((RecordReference) o).getContent());
		case "Double":
			return ((Double) getContent()).compareTo((Double) ((RecordReference) o).getContent());

		default:
			return ((Integer) getContent()).compareTo((Integer) ((RecordReference) o).getContent());
		}
	}
}
