package Potatoz;

import java.io.Serializable;

public class IndexCouple implements Serializable {
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

	private static final long serialVersionUID = 1L;
	Object first;
	Object last;

	public IndexCouple(Object first, Object last) {
		this.first = first;
		this.last = last;
	}

	public boolean inRange(int i, int counter) {
		if ((counter == 0 || i >= (Integer) first) && (last == null || i < (Integer) last))
			return true;
		return false;
	}

}
