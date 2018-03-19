package Potatoz;

import java.io.Serializable;

public class IndexCoupleReference implements Serializable {
	private static final long serialVersionUID = 1L;
	Object first;
	Object last;
	int location;

	public IndexCoupleReference(Object first, Object last, int location) {
		this.first = first;
		this.last = last;
		this.location = location;
	}

	public int getLocation() {
		return location;
	}

	public void setLocation(int location) {
		this.location = location;
	}

	public boolean inRange(int i, int counter) {
		if ((counter == 0 || i >= (Integer) first) && (last == null || i < (Integer) last))
			return true;
		return false;
	}

	public Object getFirst() {
		return first;
	}

	public void setFirst(Object first) {
		this.first = first;
	}

	public Object getLast() {
		return last;
	}

	public void setLast(Object last) {
		this.last = last;
	}

}
