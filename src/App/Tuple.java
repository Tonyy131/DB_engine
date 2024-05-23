package App;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

public class Tuple implements Serializable, Comparable<Tuple> {
	private Hashtable<String, Object> hashTable;
	private Object ck;

	public Tuple(Hashtable<String, Object> hashTable, Object ck) {
		super();
		this.hashTable = hashTable;
		this.ck = ck;
	}

	/*public String toString(){
		String str = "";

	}*/



	@Override
	public int compareTo(Tuple t) {
		Object thisCk = this.ck;
		Object otherCk = t.getCk();


		if (thisCk.getClass() != otherCk.getClass()) {
			throw new UnsupportedOperationException("Cannot compare tuples with different types of clustering keys");
		}

		if (thisCk instanceof Integer && otherCk instanceof Integer) {
			return ((Integer) thisCk).compareTo((Integer) otherCk);
		} else if (thisCk instanceof String && otherCk instanceof String) {
			return ((String) thisCk).compareTo((String) otherCk);
		} else if (thisCk instanceof Double && otherCk instanceof Double) {
			return ((Double) thisCk).compareTo((Double) otherCk);
		} else
			throw new UnsupportedOperationException(
					"Unsupported type of clustering key: " + thisCk.getClass().getName());
	}

	public Hashtable<String, Object> getHashTable() {
		return hashTable;
	}

	public void setHashTable(Hashtable<String, Object> values) {
		this.hashTable = values;
	}

	public Object getCk() {
		return ck;
	}

	public void setCk(Object ck) {
		this.ck = ck;
	}

	public Object getValue(String cName) {
		return hashTable.get(cName);
	}

	public void setValue(String cName, Object value) {
		hashTable.put(cName, value);
	}

}
