package App;

import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.Collectors;

public class DBApp {
	private static final String csvFile = "metadata.csv";
	private static final String TABLES_DIRECTORY = "tables/";

	public DBApp() {

	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {

	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		if (new File(csvFile).exists() && tableExists(strTableName)) {
			throw new DBAppException("Table already exists: " + strTableName);
		}
		Table t = new Table(strTableName, strClusteringKeyColumn, new Vector<>(htblColNameType.keySet()),
				new Vector<>(htblColNameType.values()), new Vector<>());
		createTableFolder(t);
		t.saveTable();
		writeTable(t);
	}

	private boolean tableExists(String strTableName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		String line = br.readLine();
		while (line != null) {
			String[] sp = line.trim().split(",");
			for (int i = 0; i < sp.length; i++) {
				sp[i] = sp[i].trim();
			}
			if (sp[0].equalsIgnoreCase(strTableName)) {
				br.close();
				return true;
			}
			line = br.readLine();
		}
		br.close();
		return false;
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {
		bplustree index = new bplustree(20, strColName, strIndexName);
		(Table.getTable(strTableName)).newIndex(strColName, strIndexName, index);
		updateMetadata(strTableName, strColName, strIndexName);
	}

	private void updateMetadata(String strTableName, String strColName, String strIndexName) throws DBAppException {
		BufferedReader br = null;
		FileWriter fw = null;
		try {
			File inputFile = new File(csvFile);
			File tempFile = new File("temp_metadata.csv");
			br = new BufferedReader(new FileReader(inputFile));
			fw = new FileWriter(tempFile);

			String line;
			while ((line = br.readLine()) != null) {
				String[] parts = line.trim().split(", ");
				for (int i = 0; i < parts.length; i++) {
					parts[i] = parts[i].trim();
				}
				if (parts[0].equals(strTableName) && parts[1].equals(strColName)) {
					line = String.join(", ", parts[0], parts[1], parts[2], parts[3], strIndexName, "B+Tree");
				}
				fw.write(line + "\n");
			}

			// Close the readers and writers
			br.close();
			fw.close();

			// Delete the original file
			inputFile.delete();

			// Rename the temporary file to the original file name
			tempFile.renameTo(inputFile);

		} catch (IOException e) {
			throw new DBAppException("Error updating metadata file!");
		} finally {
			try {
				// Close the readers and writers in the finally block
				if (br != null) {
					br.close();
				}
				if (fw != null) {
					fw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateInput(strTableName, htblColNameValue);
		String ck = loadClusteringKeys(strTableName);
		Object ckValue = htblColNameValue.get(ck);
		if (htblColNameValue.entrySet().stream().anyMatch(entry -> entry.getValue() == null)){
			throw new DBAppException("The tuple shouldn't contain any null values");
		}
		Tuple tuple = new Tuple(htblColNameValue, ckValue);
		(Table.getTable(strTableName)).insertInTable(tuple,false);
	}

	private Hashtable<String, String> loadMetaData(String strTableName) throws IOException {
		Hashtable<String, String> columnTypes = new Hashtable<>();
		BufferedReader br = new BufferedReader(new FileReader(csvFile)); // metadataFile should be defined

		String line;
		while ((line = br.readLine()) != null) {
			String[] sp = line.split(",");
			for (int i = 0; i < sp.length; i++) {
				sp[i] = sp[i].trim();
			}
			if (sp[0].equals("Table Name")) {
				continue;
			}
			if (sp[0].equals(strTableName)) {
				columnTypes.put(sp[1], sp[2]);
			}
		}

		br.close();
		return columnTypes;
	}

	private String loadClusteringKeys(String strTableName) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
			while ((line = br.readLine()) != null) {
				String[] sp = line.trim().split(",");
				for (int i = 0; i < sp.length; i++) {
					sp[i] = sp[i].trim();
				}
				if (sp[0].equals("Table Name")) {
					continue;
				}
				if (sp[0].equals(strTableName) && sp[3].equalsIgnoreCase("True")) {
					return sp[1];
				}
			}

			br.close();
			throw new DBAppException("Table not found!");
		} catch (IOException e) {
			throw new DBAppException("An error occurred while opening the file");
		}
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateInput(strTableName,htblColNameValue);
		Tuple t = new Tuple(htblColNameValue, clusteringKey(strClusteringKeyValue));
		Hashtable<String, Object> indices = getColumnIndices(strTableName, htblColNameValue);
		(Table.getTable(strTableName)).updateTable(t, htblColNameValue, indices);
	}

	
	public Object clusteringKey(String s){
		try{
			return Integer.parseInt(s);
		} catch (NumberFormatException e) {
            try{
				return Double.parseDouble(s);
			} catch (NumberFormatException ex) {
                return s;
            }
        }
    }
	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateInput(strTableName, htblColNameValue);
		String ck = loadClusteringKeys(strTableName);
		Table t = Table.getTable(strTableName);
		if (htblColNameValue.containsKey(ck)) {
			Object ckValue = htblColNameValue.get(ck);
			Tuple tuple = new Tuple(htblColNameValue, ckValue);
			t.deleteTupleFromTableWithCK(tuple);
		} else {
			Hashtable<String, Object> z;
			if (!t.getIndexNames().isEmpty())
				z = getColumnIndices(strTableName,htblColNameValue);
			else {
				z = new Hashtable<>();
			}
			if(!z.isEmpty()) {
				Set<String> x = z.keySet();
				Collection<Object> y = z.values();
				ArrayList<String> indicesNames = new ArrayList<>(x);
				Vector<bplustree> indices;
				try {
					indices = indicesNames.stream().map(indexName -> {
						try {
							return bplustree.getIndex(strTableName, indexName);
						} catch (DBAppException e) {
							throw new RuntimeException(e);
						}
					}).collect(Collectors.toCollection(Vector::new));
				} catch (RuntimeException e) {
					throw new DBAppException("An error occurred while retrieving the index");
				}
				Vector<String> intersection = indices.stream()
						.map(index -> index.search(z.get(index.getIndexName())))
						.reduce((result1, result2) -> {
							result1.retainAll(result2);
							return result1;
						}).orElse(new Vector<>());
				t.deleteWithIndex(intersection, htblColNameValue);
			}else {
				t.deleteTupleFromTableWithoutCK(htblColNameValue);
			}
		}

	}

	private Hashtable<String, Object> getColumnIndices(String table, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Hashtable<String, Object> columnIndices = new Hashtable<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
			while ((line = br.readLine()) != null) {
				String[] parts = line.trim().split(",");
				for (int i = 0; i < parts.length; i++) {
					parts[i] = parts[i].trim();
				}
				if (parts[0].equalsIgnoreCase("Table Name")) {
					continue;
				}
				String tableName = parts[0];
				String columnName = parts[1];
				String indexName = parts[4];
				if (tableName.equalsIgnoreCase(table) && htblColNameValue.containsKey(columnName) && indexName != null && !indexName.equals("null")) {
					columnIndices.put(indexName, htblColNameValue.get(columnName));
				}
			}
			br.close();
		} catch (IOException e) {
			throw new DBAppException("");
		}
		return columnIndices;
	}

	private String hasIndex(String table, String name) throws DBAppException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
			while ((line = br.readLine()) != null) {
				String[] parts = line.trim().split(",");
				for (int i = 0; i < parts.length; i++) {
					parts[i] = parts[i].trim();
				}
				if (parts[0].equalsIgnoreCase("Table Name")) {
					continue;
				}
				String tableName = parts[0];
				String columnName = parts[1];
				String indexName = parts[4];
				if (table.equalsIgnoreCase(tableName) && name.equalsIgnoreCase(columnName) && indexName != null && !indexName.equals("null")) {
					return indexName;
				}
			}
			br.close();
			return null;
		} catch (IOException e) {
			throw new DBAppException("");
		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms == null || strarrOperators == null || arrSQLTerms.length != strarrOperators.length + 1) {
			throw new DBAppException("Invalid SQL terms array provided!");
		}

		Vector<Tuple> finalResult = new Vector<>();
		String tableName = arrSQLTerms[0]._strTableName;
		Vector<Tuple> firstResult = executeSQL(arrSQLTerms[0]);
		if(firstResult == null)
			return null;
		finalResult.addAll(firstResult);

		for (int i = 0; i < strarrOperators.length; i++) {
			String operator = strarrOperators[i];
			SQLTerm nextTerm = arrSQLTerms[i + 1];

			Vector<Tuple> nextResult = executeSQL(nextTerm);
			if (operator.equalsIgnoreCase("AND")) {
				finalResult.retainAll(nextResult);
			} else if (operator.equalsIgnoreCase("OR")) {
				if(nextResult!=null)
					finalResult.addAll(nextResult);
			} else if (operator.equalsIgnoreCase("XOR")) {
				Vector<Tuple> temp = new Vector<>(finalResult);
				finalResult.clear();
				for (Tuple row : nextResult) {
					if (!temp.contains(row)) {
						finalResult.add(row);
					}
				}
				for (Tuple row : temp) {
					if (!nextResult.contains(row)) {
						finalResult.add(row);
					}
				}
			}

		}

		return finalResult.iterator();
	}

	private Vector<Tuple> executeSQL(SQLTerm SQLTerm) throws DBAppException {
		String tableName = SQLTerm._strTableName;
		String columnName = SQLTerm._strColumnName;
		String operator = SQLTerm._strOperator;
		Object value = SQLTerm._objValue;
		String ck = loadClusteringKeys(tableName);
		Vector<Tuple> result = new Vector<>();
		String index = hasIndex(tableName, columnName);
		if (columnName.equalsIgnoreCase(ck)) {
			result = Table.getTable(tableName).selectTuplesFromTableWithCK(columnName, operator, value);
		}else{
			if(index != null) {
				result = Table.getTable(tableName).selectTuplesFromTableWithIndex(columnName, operator, value, index);
			}else {
				try {
					result = Table.getTable(tableName).selectTuplesFromTableWithoutCK(columnName, operator, value);
				}catch (DBAppException e) {
					return null;
				}
			}
		}
		return result;
	}

	private void writeTable(Table table) throws DBAppException {
		FileWriter f = null;
		try {
			File file = new File(csvFile);
			if (!file.exists()) {
				f = new FileWriter(csvFile, true);
				f.append("Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType");
				f.append("\n");
				f.flush();
				f.close();
			}
			f = new FileWriter(csvFile, true);
			for (int i = table.getColumnNames().size() - 1; i >= 0; i--) {
				f.append(table.getStrTableName());
				f.append(", ");
				f.append(table.getColumnNames().get(i));
				f.append(", ");
				f.append(table.getColumnTypes().get(i));
				f.append(", ");
				if (table.getColumnNames().get(i).equals(table.getStrClusteringKeyColumn()))
					f.append("True");
				else
					f.append("False");
				f.append(", ");
				f.append("null, null");
				f.append("\n");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("can't find the file!");
		} finally {
			try {
				f.flush();
				f.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void createTableFolder(Table table) {
		File tableDirectory = new File(TABLES_DIRECTORY + table.getStrTableName());
		tableDirectory.mkdirs();
	}

	public void validateInput(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Hashtable<String, String> htblColNameType = null;
		try {
			htblColNameType = loadMetaData(strTableName);
		} catch (IOException e) {
			throw new DBAppException("Can't find Table!");
		}
		for (String column : htblColNameValue.keySet()) {
			if (!htblColNameType.containsKey(column)) {
				throw new DBAppException(column + " doesn't exist in " + strTableName + " table!");
			}
			String expectedType = htblColNameType.get(column);
			Object value = htblColNameValue.get(column);
			String type = value.getClass().getName();
			if (!(expectedType.equalsIgnoreCase(type)))
				throw new DBAppException("Invalid input type for " + column + " column");
		}
	}

	public static String printTuple(Tuple t){
		String str = "";
		ArrayList<String> keys = new ArrayList<>(t.getHashTable().keySet());
		for (int i = 0; i <keys.size() ; i++) {
			str += "column name : " + keys.get(i) + ",Value is " + t.getHashTable().get(keys.get(i)) + "\n" ;
		}


		return str;
	}

	public static void main(String[] args) {

		try {
			String strTableName1 = "Shop";
			DBApp dbApp = new DBApp( );
			Hashtable htblColNameType1 = new Hashtable( );
			htblColNameType1.put("Branch_id", "java.lang.Integer");
			htblColNameType1.put("B_Address", "java.lang.String");
//			dbApp.createTable( strTableName1, "Branch_id", htblColNameType1);

			String strTableName2 = "City";
			Hashtable htblColNameType2 = new Hashtable( );
			htblColNameType2.put("Name", "java.lang.String");
			htblColNameType2.put("Population", "java.lang.Integer");
//			dbApp.createTable( strTableName2, "Name", htblColNameType2);

			String strTableName3 = "Student";
			Hashtable htblColNameType3 = new Hashtable( );
			htblColNameType3.put("SID", "java.lang.Integer");
			htblColNameType3.put("Sname", "java.lang.String");
			htblColNameType3.put("SGPA", "java.lang.Double");
			htblColNameType3.put("Major", "java.lang.String");
//			dbApp.createTable( strTableName3, "SID", htblColNameType3);
			//Table table = Table.getTable(strTableName3);
//			Page page = Page.getPage(strTableName3, "Page0");
//			System.out.print(page);
			/*for(int i = 0 ; i < page.getTuples().size() ; i++){
				System.out.print(Tuple.print(page.getTuples().get(i)));
			}*/

////// First Check

			Hashtable htblColNameValue = new Hashtable( );
//			htblColNameValue.put("Name", new Integer( 2343432 ));
//			htblColNameValue.put("Population", new Integer(12000000));
//			dbApp.insertIntoTable( strTableName2 , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("Name", new String("Naples"));
//			dbApp.insertIntoTable( strTableName2 , htblColNameValue );
//
//////// exception here
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("Name", new String("Naples"));
//			htblColNameValue.put("Population", new Integer(5000000));
//			dbApp.insertIntoTable( strTableName2 , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("Name", new String("Norway"));
//			htblColNameValue.put("Population", new Integer(12000000));
//			dbApp.insertIntoTable( strTableName2 , htblColNameValue );
//
/////// print pages here
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("SID", new Integer( 4 ));
//			htblColNameValue.put("Sname", new String("Ali" ) );
//			htblColNameValue.put("SGPA", new Double( 2.1 ) );
//			htblColNameValue.put("Major", new String ( "arch" ) );
//			dbApp.insertIntoTable( strTableName3 , htblColNameValue );
//			dbApp.updateTable(strTableName3, "4", htblColNameValue);
//
///////// create page 1
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("SID", new Integer( 9 ));
//			htblColNameValue.put("Sname", new String( "Hussien" ) );
//			htblColNameValue.put("SGPA", new Double( 1.5 ) );
//			htblColNameValue.put("Major", new String ( "CS" ) );
//			dbApp.insertIntoTable( strTableName3 , htblColNameValue );

//			htblColNameValue.clear( );
//			htblColNameValue.put("SID", new Integer( 17 ));
//			htblColNameValue.put("Sname", new String( "Hoda" ) );
//			htblColNameValue.put("SGPA", new Double( 0.8 ) );
//			htblColNameValue.put("Major", new String ( "CS" ) );
//			dbApp.insertIntoTable( strTableName3 , htblColNameValue );

//			htblColNameValue.clear( );
//			htblColNameValue.put("SID", new Integer( 19 ));
//			htblColNameValue.put("Sname", new String( "Ola" ) );
//			htblColNameValue.put("SGPA", new Double( 3.1 ) );
//			htblColNameValue.put("Major", new String ( "CS" ) );
//			dbApp.insertIntoTable( strTableName3 , htblColNameValue );
//
//////// new page without shifting
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("SID", new Integer( 5 ));
//			htblColNameValue.put("Sname", new String("Samy" ) );
//			htblColNameValue.put("SGPA", new Double( 2.0 ) );
//			htblColNameValue.put("Major", new String ( "CS" ) );
//			dbApp.insertIntoTable( strTableName3 , htblColNameValue );

//////// shifting here
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("Population", new Integer(5000000));
//			dbApp.updateTable( strTableName2 , "Naples" , htblColNameValue );
//
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("Population", new String( "True" ));
//			dbApp.updateTable( strTableName2 , "Naples" , htblColNameValue );
//
//////// exception here
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("Sname", new String("Hany" ) );
//			htblColNameValue.put("SGPA", new Double( 0.79 ) );
//			dbApp.updateTable( strTableName3 , "17" , htblColNameValue );
//
//			htblColNameValue.clear( );
//
//			htblColNameValue.put("SGPA", new Double( 2.0 ) );
//			dbApp.updateTable( strTableName3 , "19" , htblColNameValue );
//
//////// does nothing
//
//
//			Table t = Table.getTable(strTableName3);
//			for(int i = 0 ; i<t.getPageNumbers().size();i++){
//				Page p = Page.getPage(strTableName3, t.getPageNumbers().get(i));
//				for(int j = 0; j <p.getTuples().size();j++){
//					System.out.println(printTuple(p.getTuples().get(j)));
//				}
//			}
//			dbApp.createIndex(strTableName3, "SID", "idIndex");
//			dbApp.createIndex(strTableName3, "SGPA", "gpaIndex");
			htblColNameValue.clear( );
//			htblColNameValue.put("SID", new Integer( 17 ));
//			htblColNameValue.put("Sname", new String("Samy" ) );
//			htblColNameValue.put("SGPA", new Double( 2.0 ) );
//			htblColNameValue.put("Major", new String ( "CS" ) );
			dbApp.deleteFromTable(strTableName3,htblColNameValue );

//			t = Table.getTable(strTableName3);
//			for(int i = 0 ; i<t.getPageNumbers().size();i++){
//				Page p = Page.getPage(strTableName3, t.getPageNumbers().get(i));
//				for(int j = 0; j <p.getTuples().size();j++){
//					System.out.println(printTuple(p.getTuples().get(j)));
//				}
//			}
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[3];
//			arrSQLTerms[2] = new SQLTerm();
//			arrSQLTerms[1] = new SQLTerm();
//			arrSQLTerms[0] = new SQLTerm();
//
//			arrSQLTerms[0]._strTableName = "Student";
//			arrSQLTerms[0]._strColumnName= "SID";
//			arrSQLTerms[0]._strOperator = "<=";
//			arrSQLTerms[0]._objValue = 4;
//
//			arrSQLTerms[1]._strTableName = "Student";
//			arrSQLTerms[1]._strColumnName= "SGPA";
//			arrSQLTerms[1]._strOperator = ">=";
//			arrSQLTerms[1]._objValue = new Double( 1.5 );
//
//			arrSQLTerms[2]._strTableName = "Student";
//			arrSQLTerms[2]._strColumnName= "SID";
//			arrSQLTerms[2]._strOperator = ">";
//			arrSQLTerms[2]._objValue = 9;
//			String[]strarrOperators = new String[2];
//			strarrOperators[0] = "OR";
//			strarrOperators[1] = "XOR";
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//			while (resultSet.hasNext()) {
//				System.out.print(printTuple((Tuple) resultSet.next()));
//			}


//
// select * from Student where name = “John Noor” or gpa = 1.5;
//
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}

}