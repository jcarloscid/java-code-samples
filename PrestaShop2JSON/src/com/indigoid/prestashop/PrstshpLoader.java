package com.indigoid.prestashop;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import com.indigoid.dbutils.MariaDBConnectionManager;
import com.indigoid.dbutils.MyPreparedStatement;
import com.indigoid.dbutils.MyStatement;
import com.indigoid.utils.JSONBuilder;
import com.indigoid.utils.MessageLogger;

/**
 * Abstract class with all the common behaviors of loaders. Loaders are classes
 * that can load into memory an object hierarchy (like customers, products or
 * orders) from a database, by running a series of SQL queries that iterates on
 * the hierarchy of each object tree.
 * 
 * Subclasses need to implement the load() method which will basically build a
 * set of DataSelector objects that represents the object hierarchy, and then
 * call loadMainLevel() to start loading the hierarchy from its root.
 * 
 * @author Charlie
 *
 */
public abstract class PrstshpLoader {

	/**
	 * A data selector represents a query to retrieve elements at one level on the
	 * object hierarchy.
	 */
	protected class DataSelector {

		private String idColumn;
		private String attribute;
		private String query;
		private boolean asAnArray;

		/**
		 * Constructor. Creates a new DataSelector and sets all its properties
		 * 
		 * @param idColumn
		 *            Name of the database column that is the id of the element
		 * @param attribute
		 *            Name of the attribute used on the JSON to represent the elements
		 *            of this level. For the Main level object, this field is stored on
		 *            the document_type property
		 * @param query
		 *            SQL query used to retrieve all objects on current level
		 * @param asAnArray
		 *            Flag indicating if the selectors retrieves a single object (false)
		 *            or a collection of objects (true)
		 */
		public DataSelector(String idColumn, String attribute, String query, boolean asAnArray) {
			this.idColumn = idColumn;
			this.attribute = attribute;
			this.query = query;
			this.asAnArray = asAnArray;
		}

		/**
		 * @return Name of the database column that is the id of the element
		 */
		public String getIdColumn() {
			return this.idColumn;
		}

		/**
		 * @return Name of the attribute used on the JSON to represent the elements of
		 *         this level. For the Main level object, this field is stored on the
		 *         document_type property
		 */
		public String getAttribute() {
			return this.attribute;
		}

		/**
		 * @return SQL query used to retrieve all objects on current level
		 */
		public String getQuery() {
			return this.query;
		}

		/**
		 * @return Flag indicating if the selectors retrieves a single object (false) or
		 *         a collection of objects (true)
		 */
		public boolean asAnArray() {
			return this.asAnArray;
		}
	}

	/**
	 * Element's id column name is changed to this in order to be used by MongoDB as
	 * document id.
	 */
	private static final String MONGODB_COLLECTION_UNIQUE_ID = "_id";
	/**
	 * JSON property for the top level document type
	 */
	private static final String DOCUMENT_TYPE_PROPERTY = "document_type";
	/**
	 * JSON property for the top level shop name
	 */
	private static final String SHOP_NAME_PROPERTY = "shop_name";
	/**
	 * This predicate means load all elements.
	 */
	private static final String PREDICATE_ALL_ELEMENTS = "";
	/**
	 * This is the condition used to select a specific group of elements from the
	 * database.
	 */
	private String predicate;
	/**
	 * All elements data. Data is stored as a hash map. The key is the <i>_id</i>
	 * and the value is the JSON representation of the element data.
	 */
	private HashMap<Integer, String> elements = new HashMap<Integer, String>();

	/**
	 * Creates a loader for all elements.
	 */
	public PrstshpLoader() {
		this.predicate = PREDICATE_ALL_ELEMENTS;
	}

	/**
	 * Creates a loader for elements fulfilling a particular predicate
	 * 
	 * @param predicate
	 *            Condition that the selected elements must follow.
	 */
	public PrstshpLoader(String predicate) {
		this.predicate = predicate;
	}

	/**
	 * Load elements information from a PrestaShop database. The implementation of
	 * this method should call to loadMainLevel() after setting the appropriate
	 * DataSelector(s) required to load the main level elements as well as all child
	 * elements.
	 * 
	 * @param con
	 *            The database manager connection
	 * @param shopName
	 *            Name of the shop to identify generated top-level documents
	 * @return The number of elements loaded.
	 */
	public abstract int load(com.indigoid.dbutils.MariaDBConnectionManager con, String shopName);

	/**
	 * Load the elements on the main level of the hierarchy and for each element
	 * loaded, load all its descendants.
	 * 
	 * @param con
	 *            The database manager connection
	 * @param shopName
	 *            Name of the shop to identify generated top-level documents
	 * @param mainSelector
	 *            This is the selector for the main level. It must be a query
	 *            without parameters.
	 * @param selectors
	 *            These are the selectors for the child elements. These selectors
	 *            use a query with one parameter that is the parent' key element
	 *            value.
	 * @return The number of elements loaded.
	 */
	protected int loadMainLevel(MariaDBConnectionManager con, String shopName, DataSelector mainSelector,
			DataSelector[][] selectors) {

		MyStatement stmt = null;
		Integer elementId = null;
		int nCols, nElements, index;
		String keyColumn = mainSelector.getIdColumn();
		String shopNameHash = "/" + shopName.hashCode() + "/";

		// Build the query with or without predicate
		StringBuilder query = new StringBuilder();
		query.append(mainSelector.getQuery());
		if (!this.predicate.equals(PREDICATE_ALL_ELEMENTS)) {
			query.append(" WHERE ").append(predicate);
		}

		// Nothing loaded so far
		nElements = 0;

		try {
			// Get a statement
			stmt = con.acquireStatement();

			// Execute the query to retrieve top level elements
			ResultSet rs = stmt.executeQuery(query.toString());

			// Retrieve metadata
			ResultSetMetaData md = rs.getMetaData();
			nCols = md.getColumnCount();

			// From metadata retrieve column names and types
			String[] colNames = new String[nCols + 1];
			int[] colTypes = new int[nCols + 1];
			for (index = 1; index <= nCols; index++) {
				colNames[index] = md.getColumnLabel(index);
				colTypes[index] = md.getColumnType(index);
			}

			// Main loop: Iterate over each row
			while (rs.next()) {

				// Create a JSON for this element
				JSONBuilder json = new JSONBuilder(0);

				// Added properties (top-level documents only)
				json.appendString(DOCUMENT_TYPE_PROPERTY, mainSelector.getAttribute());
				json.appendString(SHOP_NAME_PROPERTY, shopName);

				// For each column (1..nCols)
				for (index = 1; index <= nCols; index++) {

					// If current column is key column, store the index value, so it can be used at
					// the end of the loop as the key to the hash map.
					// Also add an "_id" property to conform MongoDB standards. The _id is created
					// using this element id plus the hash code of the shop name (enclosed into / /)
					String colName = colNames[index];
					if (colName.equals(keyColumn)) {
						elementId = rs.getInt(index);
						json.appendString(MONGODB_COLLECTION_UNIQUE_ID, elementId + shopNameHash);
					}

					// Convert this column to JSON format
					data2JSON(rs, index, colName, colTypes[index], json);
				}

				// Load sub-levels
				if (selectors != null) {
					for (index = 0; index < selectors.length; index++) {
						loadChildElements(con, elementId, json, 1, selectors[index]);
					}
				}

				// Store the final JSON on the hash map
				elements.put(elementId, json.toString());

				// One more element loaded
				nElements++;
			}

		} catch (SQLException e) {
			MessageLogger.logUnmanagedException(e);
		} finally {
			try {
				con.relaseStatement(stmt);
			} catch (SQLException e) {
				/* Ignore error */ }
		}

		// Number of elements loaded on this instance
		return nElements;
	}

	/**
	 * Use this method to retrieve those child elements of a parent object that are
	 * simply converted into JSON as as single object or as an array of objects
	 * (depending on asAnArray() on the selector). <br/>
	 * <br/>
	 * A query to retrieve those elements from the database is provided (into
	 * DataSelector). This query must have a parameter with the parent id
	 * (integer).<br/>
	 * <br/>
	 * The method is designed to work recursively on a list of descendant queries
	 * over the object hierarchy. <i>selectors</i> represents the array of the
	 * levels to process. The first element represents current level, and if there
	 * are more levels, they are processed recursively.
	 * 
	 * @param con
	 *            Database connection manager.
	 * @param id
	 *            Id of the element to process in current level (parent's id).
	 * @param parentJson
	 *            JSON representation of the parent object.
	 * @param selectors
	 *            An array of selector representing the levels to process in the
	 *            hierarchy. Each selector contains three elements: the name of the
	 *            key field for this level, the title for the descendant elements on
	 *            the JSON array, and the query to generate the array of children
	 *            from current value of the key column (might be empty).
	 * @return The number of child elements processed for current parent.
	 */
	private static int loadChildElements(MariaDBConnectionManager con, Integer id, JSONBuilder parentJson, int level,
			DataSelector[] selectors) {

		// If there are no selectors, nothing needs to be done
		if (selectors.length == 0) {
			return 0;
		}

		MyPreparedStatement stmt = null;
		int nCols, nItems, index, childKeyValue;
		String lastValue, childKeyColumn = null;
		DataSelector currentSelector = selectors[0];
		DataSelector[] restOfSelectors = null;

		// If there are more than one selector then we need to process sub-children.
		boolean withSubchildren = (selectors.length > 1);
		if (withSubchildren) {
			// Set the key column for the next level
			childKeyColumn = selectors[1].getIdColumn();

			// Compute the array with all selectors but first, to work recursively
			restOfSelectors = new DataSelector[selectors.length - 1];
			for (int i = 1; i < selectors.length; i++) {
				restOfSelectors[i - 1] = selectors[i];
			}
		}

		// Nothing processed yet
		nItems = 0;

		try {
			// Get a prepared statement
			stmt = con.acquirePreparedStatement(currentSelector.getQuery());

			// Set the parameter
			stmt.setInt(1, id);

			// Execute the query to retrieve children elements
			ResultSet rs = stmt.executeQuery();

			// Retrieve metadata
			ResultSetMetaData md = rs.getMetaData();
			nCols = md.getColumnCount();

			// From metadata retrieve column names and types
			String[] colNames = new String[nCols + 1];
			int[] colTypes = new int[nCols + 1];
			for (index = 1; index <= nCols; index++) {
				colNames[index] = md.getColumnLabel(index);
				colTypes[index] = md.getColumnType(index);
			}

			// Main loop: Iterate over each row
			while (rs.next()) {

				// This is written as an array of objects with the label set on the selector
				if (nItems == 0) {
					if (currentSelector.asAnArray()) {
						parentJson.openArray(currentSelector.getAttribute());
					} else {
						parentJson.setProperty(currentSelector.getAttribute());
					}
				}

				// Create a JSON for this object
				JSONBuilder json = new JSONBuilder(level);

				// For each column (1..nCols)
				for (childKeyValue = -1, index = 1; index <= nCols; index++) {
					// Convert this column to JSON format
					lastValue = data2JSON(rs, index, colNames[index], colTypes[index], json);

					// If we need to process sub-children, check if this is the key column
					// and save the value for the key of the next level
					if (withSubchildren && childKeyValue < 0 && lastValue != null) {
						if (colNames[index].equals(childKeyColumn)) {
							childKeyValue = Integer.parseInt(lastValue);
						}
					}
				}

				// If we need to process sub-children, and we found the key value, process the
				// next level recursively
				if (withSubchildren && childKeyValue > 0) {
					loadChildElements(con, childKeyValue, json, level + 1, restOfSelectors);
				}

				// Add address JSON to parent one
				parentJson.appendUnnamedJSON(json.toString());

				// One more address processed
				nItems++;
			}

			// Close the array on the parent
			if (nItems > 0 && currentSelector.asAnArray()) {
				parentJson.closeArray();
			}

		} catch (SQLException e) {
			MessageLogger.logUnmanagedException(e);
		} finally {
			try {
				con.relasePreparedStatement(stmt);
			} catch (SQLException e) {
				/* Ignore error */ }
		}

		// Number of description processed
		return nItems;
	}

	/**
	 * Convert one element from current row on a result set to JSON, depending on
	 * the column type. NULL values are not passed to the JSON object. Not all the
	 * possible SQL types has being implemented here.
	 * 
	 * @param rs
	 *            Result set containing current row.
	 * @param index
	 *            Index of the data element to be processed
	 * @param colName
	 *            Name of the column
	 * @param colType
	 *            Type of the column
	 * @param json
	 *            JSON builder object to which the column is added.
	 * @return null if the field value was NULL, or the value of the field (as a
	 *         string) otherwise.
	 * @throws SQLException
	 *             When an error occurss trying to retrieve a column.
	 */
	private static String data2JSON(ResultSet rs, int index, String colName, int colType, JSONBuilder json)
			throws SQLException {

		switch (colType) {
		//
		// Integers
		//
		case java.sql.Types.BIT:
		case java.sql.Types.TINYINT:
		case java.sql.Types.INTEGER:
			int intValue = rs.getInt(index);
			if (!rs.wasNull()) {
				json.appendInt(colName, intValue);
				return new Integer(intValue).toString();
			}
			break;
		//
		// Decimal numbers
		//
		case java.sql.Types.DECIMAL:
		case java.sql.Types.REAL: /* float */
			String decValue = rs.getString(index);
			if (!rs.wasNull()) {
				json.appendDecimal(colName, decValue);
				return decValue;
			}
			break;
		//
		// Strings (and text)
		//
		case java.sql.Types.CHAR: /* enums */
		case java.sql.Types.VARCHAR: /* text */
			String strValue = rs.getString(index);
			if (!rs.wasNull()) {
				json.appendString(colName, strValue);
				return strValue;
			}
			break;
		//
		// Timestamp (and datetime)
		//
		case java.sql.Types.TIMESTAMP: /* datetime */
			Timestamp tsValue = rs.getTimestamp(index);
			if (!rs.wasNull()) {
				LocalDateTime localTS = LocalDateTime.ofInstant(tsValue.toInstant(), ZoneId.systemDefault());
				String tsStrValue = localTS.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "Z";
				json.appendDate(colName, tsStrValue);
				return tsStrValue;
			}
			break;
		//
		// Dates
		//
		case java.sql.Types.DATE:
			java.sql.Date dateValue = rs.getDate(index);
			if (!rs.wasNull()) {
				String dateStrValue = dateValue.toString() + "T00:00:00Z";
				json.appendDate(colName, dateStrValue);
				return dateStrValue;
			}
			break;
		//
		// If there is no implementation for a column type an exception is thrown
		//
		default:
			throw new TypeNotPresentException(new Integer(colType).toString(), null);
		}

		// If this point is reached, the field has a NULL value
		return null;
	}

	/**
	 * Dumps the content of the memory collection of objects loaded into a file. All
	 * JSON documents are written to the same file just one after the other.
	 * 
	 * @param fileName
	 *            Name of the output file
	 * @throws FileNotFoundException
	 *             If the given file object does not denote an existing, writable
	 *             regular file and a new regular file of that name cannot be
	 *             created, or if some other error occurs while opening or creating
	 *             the file
	 */
	public void dumpData(String fileName) throws FileNotFoundException {
		PrintStream ps = new PrintStream(fileName);
		elements.keySet().forEach(k -> ps.println(elements.get(new Integer(k))));
		ps.close();
	}
}