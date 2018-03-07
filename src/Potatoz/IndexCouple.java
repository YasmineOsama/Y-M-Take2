package Potatoz;

import java.io.Serializable;

public class IndexCouple {
	private static final long serialVersionUID = 1L;
	Object first;
	Object last;

	public IndexCouple(Object first, Object last) {
		this.first = first;
		this.last = last;
	}

	public boolean inRange(int i) {
		if (i >= (Integer) first && i <= (Integer) last)
			return true;
		return false;
	}

}
