package App;

import java.io.*;
import java.util.Vector;
import java.util.Hashtable;

public class Table implements Serializable {
	private String strTableName;
	private String strClusteringKeyColumn;
	private Vector<String> columnNames;
	private Vector<String> columnTypes;
	private Vector<String> indexNames;
	private Vector<String> pageNumbers;
	private Hashtable<String,Tuple> pageMax;
	private Hashtable<String,Tuple> pageMin;

	public Table(String strTableName, String strClusteringKeyColumn, Vector<String> columnNames,
			Vector<String> columnTypes, Vector<String> indexNames) throws DBAppException {
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.columnNames = columnNames;
		validColNameType(columnTypes);
		this.columnTypes = columnTypes;
		this.indexNames = indexNames;
		pageNumbers = new Vector<>();
		pageMin = new Hashtable<>();
		pageMax = new Hashtable<>();
	}

	private void validColNameType(Vector<String> a) throws DBAppException {
        for (String s : a) {
            if (!(s.equalsIgnoreCase("java.lang.Integer") || s.equalsIgnoreCase("java.lang.String")
                    || s.equalsIgnoreCase("java.lang.Double")))
                throw new DBAppException("Unsupported column type");

        }
	}

	public Vector<String> getPageNumbers() {
		return pageNumbers;
	}

	public void setPageNumbers(Vector<String> pageNumbers) {
		this.pageNumbers = pageNumbers;
	}

	public void setStrTableName(String strTableName) {
		this.strTableName = strTableName;
	}

	public void setStrClusteringKeyColumn(String strClusteringKeyColumn) {
		this.strClusteringKeyColumn = strClusteringKeyColumn;
	}

	public void setColumnNames(Vector<String> columnNames) {
		this.columnNames = columnNames;
	}

	public void setColumnTypes(Vector<String> columnTypes) {
		this.columnTypes = columnTypes;
	}

	public void setIndexNames(Vector<String> indexNames) {
		this.indexNames = indexNames;
	}

	public void setPageMax(Hashtable<String, Tuple> pageMax) {
		this.pageMax = pageMax;
	}

	public void setPageMin(Hashtable<String, Tuple> pageMin) {
		this.pageMin = pageMin;
	}

	public String getStrTableName() {
		return strTableName;
	}

	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public Vector<String> getColumnNames() {
		return columnNames;
	}

	public Vector<String> getColumnTypes() {
		return columnTypes;
	}

	public Vector<String> getIndexNames() {
		return indexNames;
	}

	public Hashtable<String, Tuple> getPageMax() {
		return pageMax;
	}

	public Hashtable<String, Tuple> getPageMin() {
		return pageMin;
	}

	public void insertInTable(Tuple t, boolean ex) throws DBAppException {
		int loc = binarySearch(t);
		if(ex){
			loc++;
		}
		String pageName = "Page" + loc;
		if(pageNumbers.isEmpty() || loc>=pageNumbers.size()){
			Page p = new Page(pageName, strTableName );
			pageNumbers.add(pageName);
			pageMax.put(pageName,t);
			pageMin.put(pageName,t);
			p.insertInPage(t);
			this.insertInIndices(t,pageName);
		}else{
			Page p = Page.getPage(this.getStrTableName(), pageName);
			Tuple extra = p.insertInPage(t);
			pageMax.replace(pageName,p.getMax());
			pageMin.replace(pageName,p.getMin());
			if(extra!=null){
				this.deleteFromIndices(extra,pageName);
				insertInTable(extra, true);
			}
			this.insertInIndices(t,pageName);
		}
		saveTable();
	}

	public void deleteWithIndex(Vector<String> intersection, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		for (String pageName : intersection) {
			try{
				Page p = Page.getPage(this.strTableName, pageName);
				if(p == null)
					return;
				p.deleteFromPageWithoutCK(this, htblColNameValue);
			}catch(DBAppException e){
				return;
			}

		}
		saveTable();
	}

	public void deleteTupleFromTableWithCK(Tuple tuple) throws DBAppException {
		int loc = binarySearch(tuple);
		if (pageNumbers.get(loc) == null)
			throw new DBAppException("Tuple can't be found!");
		String pageName = "Page" + loc;
		Page p = Page.getPage(this.strTableName, pageName);
		p.deleteFromPageWithCK(this,tuple);
		this.deleteFromIndices(tuple, pageName);
		saveTable();
	}

	public void deleteTupleFromTableWithoutCK(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		for(String pageName : pageNumbers){
			Page p = Page.getPage(this.strTableName, pageName);
			p.deleteFromPageWithoutCK(this, htblColNameValue);
		}
		saveTable();
	}

	private int binarySearch(Tuple t) throws DBAppException {
		int n = this.getPageNumbers().size();
		int low = 0;
		int high = n - 1;

		while (low <= high) {
			int mid = (low + high) / 2;
			String pageName = this.getPageNumbers().get(mid);
            int compareWithMin = t.compareTo(this.pageMin.get(pageName));
			int compWithMax = t.compareTo(this.pageMax.get(pageName));
			if (compWithMax <= 0 && compareWithMin >= 0) {
				return mid;
			} else if (compWithMax > 0)
				low = mid + 1;
			else
				high = mid - 1;
		}
		if(low == -1 || n-1 == -1)
			return 0;
		return Math.min(low, n - 1);
	}

	public void saveTable() throws DBAppException {
		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("tables/" + strTableName + "/table.bin"))) {
			oos.writeObject(this);
		} catch (IOException e) {
			throw new DBAppException("can't save table");
        }

    }

	public static Table getTable(String name) throws DBAppException {
		String fileName = "tables/" + name + "/table.bin";

		try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
			return (Table) ois.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("can't save table");
		}
	}

	public void deletePage(String pageName){
		String fileName = "tables/" + this.strTableName + "/page" + pageName +".bin";
		this.pageNumbers.remove(pageName);
		this.pageMin.remove(pageName);
		this.pageMax.remove(pageName);
		File file = new File(fileName);
		file.delete();
	}

	public void updateTable(Tuple t, Hashtable<String, Object> htblColNameValue, Hashtable<String, Object> indices) throws DBAppException {
		int loc = binarySearch(t);
		String pageName = "Page" + loc;
		Page p = Page.getPage(this.strTableName, pageName);
		p.updateTuples(t, htblColNameValue, indices);
		saveTable();
	}



	public void newIndex(String strColName, String strIndexName, bplustree index) throws DBAppException {
		for(String pageName: pageNumbers){
			Page p = Page.getPage(this.strTableName,pageName);
			p.FillNewIndex(index, strColName);
		}
		this.indexNames.add(strIndexName);
		index.saveIndex(this.strTableName, strIndexName);
		saveTable();
	}

	public void insertInIndices(Tuple t, String pageName) throws DBAppException {
		for(String indexName: indexNames){
			bplustree index = bplustree.getIndex(this.strTableName,indexName);
			index.insert(t.getValue(index.getStrColName()),pageName);
			index.saveIndex(this.strTableName, indexName);
		}
	}

	public void deleteFromIndices(Tuple t, String pageName) throws DBAppException {
		for(String indexName: indexNames){
			bplustree index = bplustree.getIndex(this.strTableName,indexName);
			index.delete(t.getValue(index.getStrColName()),pageName);
			index.saveIndex(this.strTableName, indexName);
		}
	}

	public Vector<Tuple> selectTuplesFromTableWithIndex(String columnName, String operator, Object value, String indexName) throws DBAppException {
		Vector<Tuple> result = new Vector<>();
		bplustree index = bplustree.getIndex(this.strTableName,indexName);
		Vector<String> pages = index.search(value);
		if(pages == null){
			return null;
		}
		for(String pageName: pages){
			Page page = Page.getPage(this.strTableName, pageName);
			result.addAll(page.selectTuplesFromPageWithoutCK(columnName, operator, value));
		}
		return result;
	}

	public Vector <Tuple> selectTuplesFromTableWithCK(String columnName, String operator, Object value) throws DBAppException {
		Vector<Tuple> result = new Vector<>();
		Tuple t = new Tuple(new Hashtable<>(), value);
		int loc = binarySearch(t);
		String pageName = this.getPageNumbers().get(loc);
		switch (operator) {
			case ">": result.addAll(greaterThanOperator(columnName, value, pageName)); break;
			case ">=": result.addAll(greaterThanOperator(columnName, value, pageName));
						result.add(equalOperator(columnName, value, pageName)); break;
			case "<": result.addAll(lessThanOperator(columnName, value, pageName)); break;
			case "<=": result.addAll(lessThanOperator(columnName, value, pageName));
						result.add(equalOperator(columnName, value, pageName)); break;
			case "!=": result = selectTuplesFromTableWithoutCK(columnName, operator, value); break;
			case "=" : result.add(equalOperator(columnName, value, pageName)); break;
		}
		return result;
	}

	public Vector<Tuple> selectTuplesFromTableWithoutCK(String columnName, String operator, Object value) throws DBAppException {
		Vector<Tuple> result = new Vector<>();
		for(int i = 0 ; i<pageNumbers.size(); i++){
			String pageName = pageNumbers.get(i);
			Page page = Page.getPage(this.strTableName, pageName);
			result.addAll(page.selectTuplesFromPageWithoutCK(columnName, operator, value));
		}
		return result;
	}

	private Vector<Tuple> greaterThanOperator (String columnName, Object value, String pageName) throws DBAppException {
		Vector<Tuple> result = new Vector<>();
		for(int i = pageNumbers.indexOf(pageName); i < pageNumbers.size(); i++) {
			pageName = pageNumbers.get(i);
			Page page = Page.getPage(this.strTableName, pageName);
			result.addAll(page.selectTuplesFromPageWithCK(columnName, ">", value));
		}
		return result;
	}

	private Tuple equalOperator(String columnName, Object value, String pageName) throws DBAppException {
		Page page = Page.getPage(this.strTableName, pageName);
		return page.selectSingleTupleFromPageWithCK(columnName, value);
	}

	private Vector<Tuple> lessThanOperator(String columnName, Object value, String pageName) throws DBAppException {
		Vector<Tuple> result = new Vector<>();
		for(int i =0; i <= pageNumbers.indexOf(pageName); i++){
			pageName = pageNumbers.get(i);
			Page page = Page.getPage(this.strTableName, pageName);
			result.addAll(page.selectTuplesFromPageWithCK(columnName, "<", value));
		}
		return result;
	}

}
