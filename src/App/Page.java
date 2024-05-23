package App;

import java.io.*;
import java.util.*;

public class Page implements Serializable {
	private String pageName;
	private final String tableName;
	private int n;
	private Vector<Tuple> tuples;

	public Page(String pageName, String tableName) throws DBAppException {
		this.pageName = pageName;
		this.n = readN();
		this.tuples = new Vector<Tuple>();
		this.tableName = tableName;
	}

	public String toString() {
		String str = "";
		for (int i = 0; i < tuples.size(); i++) {
			//str += tuples.get(i);
			Tuple t = tuples.get(i);
			for(int j = 0; j < t.getHashTable().size(); j++) {
				str += t.getHashTable().get(i) + " ," + t.getHashTable().keySet().toArray()[i] + "---";
			}
			str += "\n";
		}
		return str;
	}

	public static int readN() throws DBAppException {
		String N = System.getProperty("MaximumRowsCountinPage");

		if (N == null) {
			try {
				// Use ClassLoader to access resource from package
				ClassLoader classLoader = Page.class.getClassLoader();
				InputStream inputStream = classLoader.getResourceAsStream("resources/DBApp.config");
				if (inputStream == null) {
					throw new DBAppException("Configuration file not found!");
				}

				Properties properties = new Properties();
				properties.load(inputStream);
				N = properties.getProperty("MaximumRowsCountinPage");
			} catch (IOException e) {
				throw new DBAppException("Error with configuration file!");
			}
		}
		try {
			return Integer.parseInt(N);
		} catch (NumberFormatException e) {
			throw new DBAppException("MaximumRowsCountinPage invalid format!");
		}
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	public Vector<Tuple> getTuples() {
		return tuples;
	}

	public void setTuples(Vector<Tuple> tuples) {
		this.tuples = tuples;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public Tuple insertInPage(Tuple t) throws DBAppException {
		if (this.tuples.isEmpty()) {
			tuples.add(t);
		} else {
			int location = binarySearch(this.tuples, t, 0, this.tuples.size() - 1);
			if (this.tuples.size() == this.n) {
				tuples.add(location, t);
				Tuple removed = this.tuples.remove(this.tuples.size() - 1);
				savePage();
				return removed;
			}
			tuples.add(location, t);
		}
		savePage();
		return null;
	}

	public void deleteFromPageWithCK(Table t, Tuple tuple) throws DBAppException {
		int loc = binarySearch(this.tuples, tuple, 0, this.tuples.size() - 1);
		if (tuples.get(loc).getCk().equals(tuple.getCk())) {
			this.tuples.remove(loc);
			movePagesTuples(t, this);
		} else {
			throw new DBAppException("Can't find Tuple!");
		}
	}

	public void deleteFromPageWithoutCK(Table t, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		for(int i = 0; i<this.tuples.size(); i++){
			Tuple tuple = tuples.get(i);
			if (compareHashTables(htblColNameValue, tuple)){
				this.tuples.remove(tuple);
				movePagesTuples(t, this);
				t.deleteFromIndices(tuple,this.pageName);
			}
		}
	}

	private boolean compareHashTables(Hashtable<String, Object> t1, Tuple t2) {
		if (t2.getHashTable().size() < t1.size()) {
			return false;
		}

		for (Object key : t1.keySet()) {
			if (!t2.getHashTable().containsKey(key)) {
				return false;
			}
			if (!t1.get(key).equals(t2.getHashTable().get(key))) {
				return false;
			}
		}
		return true;
	}

	private static void movePagesTuples(Table t, Page page) throws DBAppException {
		boolean isLastPage = t.getPageNumbers().get(t.getPageNumbers().size() - 1).equals(page.pageName);

		if (isLastPage) {
			if (page.isEmpty()) {
				t.deletePage(page.getPageName());
			} else {
				Hashtable<String, Tuple> pageBound = t.getPageMax();
				pageBound.replace(page.pageName, page.getMax());
				t.setPageMax(pageBound);
				pageBound = t.getPageMin();
				pageBound.replace(page.pageName, page.getMin());
				t.setPageMin(pageBound);
				page.savePage();
			}
			return;
		}
		if (!page.isEmpty()) {
			Page nextPage = getPage(t.getStrTableName(),
					t.getPageNumbers().get(t.getPageNumbers().indexOf(page.getPageName()) + 1));
			Tuple tuple = nextPage.tuples.remove(0);
			page.tuples.add(tuple);
			Hashtable<String, Tuple> pageBound = t.getPageMax();
			pageBound.replace(page.pageName, page.getMax());
			t.setPageMax(pageBound);
			pageBound = t.getPageMin();
			pageBound.replace(page.pageName, page.getMin());
			t.setPageMin(pageBound);
			for(String indexName : t.getIndexNames()){
				bplustree index = bplustree.getIndex(t.getStrTableName(), indexName);
				//index.updateValue(tuple.getValue(index.getStrColName()), nextPage.pageName, page.pageName);
				index.delete(tuple.getValue(index.getStrColName()),nextPage.pageName);
				index.insert(tuple.getValue(index.getStrColName()),page.pageName);
				index.saveIndex(t.getStrTableName(), indexName);
			}
			page.savePage();
			movePagesTuples(t, nextPage);
		}
	}

	private static int binarySearch(Vector<Tuple> a, Tuple t, int min, int max) {
		if (min > max)
			return min;
		int mid = min + (max - min) / 2;
		if (a.get(mid).compareTo(t) == 0) {
			return mid;
		} else {
			if (a.get(mid).compareTo(t) > 0) {
				return binarySearch(a, t, min, mid - 1);
			} else
				return binarySearch(a, t, mid + 1, max);
		}
	}

	public boolean isEmpty() {
		return tuples.isEmpty();
	}

	public boolean isFull() {
		return tuples.size() >= n;
	}

	public Tuple getMin() {
		return tuples.get(0);
	}

	public Tuple getMax() {
		return tuples.get(tuples.size() - 1);
	}

	public String getTableName() {
		return tableName;
	}

	public void savePage() throws DBAppException {
		String fileName = "tables/" + this.getTableName() + "/page" + this.getPageName() + ".bin";

		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
			oos.writeObject(this);
		} catch (IOException e) {
			throw new DBAppException("Can't save page!");
		}
	}

	public static Page getPage(String tableName, String pageName) throws DBAppException {
		String fileName = "tables/" + tableName + "/page" + pageName + ".bin";
		try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
			return (Page) ois.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("can't save page");
		}
	}

	public void updateTuples(Tuple t, Hashtable<String, Object> htblColNameValue, Hashtable<String, Object> indices) throws DBAppException {
		int loc = binarySearch(this.tuples, t, 0, this.tuples.size()-1);
		if(tuples.get(loc).getCk().equals(t.getCk())){
			Hashtable<String, Object> updatedHashTable = tuples.get(loc).getHashTable();
			if(!indices.isEmpty())
				this.updateIndices(tuples.get(loc), pageName, indices);
			for (String key : htblColNameValue.keySet()) {
				Object value = htblColNameValue.get(key);
				updatedHashTable.put(key, value);
			}
			tuples.get(loc).setHashTable(updatedHashTable);
			savePage();
		}
		else{
			throw new DBAppException("Tuple with this clustering key doesn't exist!");
		}
	}



	private void updateIndices(Tuple t, String pageName, Hashtable<String, Object> indices) throws DBAppException {
		for(String indexName : indices.keySet()){
			bplustree index = bplustree.getIndex(this.tableName,indexName);
			index.delete(t.getValue(index.getStrColName()), pageName);
			index.insert(indices.get(indexName), pageName);
			index.saveIndex(this.tableName, indexName);
		}
	}

	public void FillNewIndex(bplustree index, String strColName) {
		for(Tuple t : tuples){
			index.insert(t.getHashTable().get(strColName),pageName);
		}
	}

	public Vector<Tuple> selectTuplesFromPageWithoutCK(String columnName, String operator, Object value) throws DBAppException {
		Vector<Tuple> result = new Vector<>();
		for(Tuple t : tuples){
			Object columnValue = t.getHashTable().get(columnName);
			if(columnValue == null)
				throw new DBAppException("Invalid column name");
			boolean correct = false;
			switch (operator) {
				case ">": correct = ((Comparable)columnValue).compareTo(value) > 0; break;
				case ">=": correct = ((Comparable)columnValue).compareTo(value) >= 0; break;
				case "<": correct = ((Comparable)columnValue).compareTo(value) < 0; break;
				case "<=": correct = ((Comparable)columnValue).compareTo(value) <= 0; break;
				case "!=": correct = ((Comparable) columnValue).compareTo(value) != 0; break;
				case "=": correct = ((Comparable) columnValue).compareTo(value) == 0; break;
			}
			if (correct) {
				result.add(t);
			}
		}
		return result;
	}

	public Vector<Tuple> selectTuplesFromPageWithCK(String columnName, String operator, Object value) throws DBAppException {
		Vector<Tuple> result = new Vector<>();
		boolean cont = true;
		for(Tuple t : tuples){
			if(!cont){
				break;
			}
			Object columnValue = t.getHashTable().get(columnName);
			if(columnValue == null)
				throw new DBAppException("Invalid column name");
			boolean correct = false;
			switch (operator) {
				case ">": if(((Comparable)columnValue).compareTo(value) > 0){
					correct = true;
				} else{
					cont = false;
				}
					break;
				case "<": if(((Comparable)columnValue).compareTo(value) < 0){
					correct = true;
				} else{
						cont = false;
				}
					break;
			}
			if (correct) {
				result.add(t);
			}
		}
		return result;
	}

	public Tuple selectSingleTupleFromPageWithCK(String columnName, Object value) {
		int loc = binarySearch(this.tuples, new Tuple(new Hashtable<>(), value), 0, this.tuples.size() - 1);
		if (tuples.get(loc).getCk().equals(value)){
			return tuples.get(loc);
		}else{
			return null;
		}
	}
}
