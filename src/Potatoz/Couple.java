package Potatoz;

import java.io.Serializable;

public class Couple implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String key;
	Object value;

	public Couple() {
		this.key = null;
		this.value = null;
	}
	public Couple(String key, Object value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
}
