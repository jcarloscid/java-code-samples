package com.indigoid.dbutils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import com.indigoid.utils.MessageLogger;

/**
 * This is a utility class used to hold all the resources created against the
 * database manager. It holds the connection, as well as the statements as well
 * as the result sets. The close method must be called upon program completion
 * in order to free all resources.
 * 
 * @author Charlie
 */
public class MariaDBConnectionManager implements AutoCloseable {

	//
	// Constants
	//
	private static final String MARIADB_DRIVER = "mariadb";
	private static final String JDBC_PROTOCOL = "jdbc";
	/**
	 * Initial number of statements on the pool.
	 */
	private static final int INITIAL_STATEMENTS = 5;

	/**
	 * Connection to PrestaShop database
	 */
	private Connection psDBConnection = null;
	/**
	 * SQL Statements pool
	 */
	private ArrayList<MyStatement> statementsPool = new ArrayList<>();
	/**
	 * SQL Prepared Statements pool
	 */
	private ArrayList<MyPreparedStatement> preparedStatementsPool = new ArrayList<>();

	/**
	 * Initializes resources and creates (opens) the database connection.
	 * 
	 * @param host
	 *            Database server host name
	 * @param port
	 *            Database server port number
	 * @param database
	 *            Database including PrestaShop tables
	 * @param dbUser
	 *            User to connect to the database server (must have read access to
	 *            PrestaShop database)
	 * @param dbPasswd
	 *            Password of the database user
	 * @throws SQLException 
	 */
	public MariaDBConnectionManager(String host, int port, String database, String dbUser, String dbPwd) throws SQLException {

		// Build the connection string
		String dbURL = JDBC_PROTOCOL + ":" + MARIADB_DRIVER + "://" + host + ":" + port + "/" + database;

		// Connect to the database manager
		psDBConnection = DriverManager.getConnection(dbURL, dbUser, dbPwd);

		// Create the initial pool of statements
		for (int i = 0; i < INITIAL_STATEMENTS; i++) {
			statementsPool.add(new MyStatement(this.psDBConnection));
		}
	}

	/**
	 * Acquires one statement from the pool in order to be used by a consumer. If
	 * all of the statement on the pool are in use, a new statement is created.
	 * 
	 * @return A statement that can be used to execute database calls. Its status is
	 *         <i>BUSY</i>
	 * @throws SQLException
	 *             When an error occurs creating a new statement.
	 */
	public MyStatement acquireStatement() throws SQLException {

		int nStatements = statementsPool.size();
		MyStatement stmt = null;

		// Try to find an usable statement already allocated in the pool
		for (int index = 0; index < nStatements; index++) {
			stmt = statementsPool.get(index);
			if (stmt.isUsable()) {
				stmt.setStatus(ResourceStatus.BUSY);
				return stmt; // This can be used
			}
		}

		// If no one is available, create a new statement, add it to the pool
		stmt = new MyStatement(this.psDBConnection);
		statementsPool.add(stmt);

		stmt.setStatus(ResourceStatus.BUSY);
		return stmt; // Brand new statement ready to be used.
	}

	/**
	 * Acquires one prepared statement from the pool in order to be used by a
	 * consumer. If all of the statement on the pool are in use, a new statement is
	 * created. If an existing prepared statement is reused, it must be associated
	 * with the exact same query.
	 * 
	 * @param query
	 *            Parameterized query associated to this statement
	 * @return A statement that can be used to execute database calls. Its status is
	 *         <i>BUSY</i>
	 * @throws SQLException
	 *             When an error occurs creating a new statement.
	 */
	public MyPreparedStatement acquirePreparedStatement(String query) throws SQLException {

		int nStatements = preparedStatementsPool.size();
		MyPreparedStatement stmt = null;

		// Try to find an re-usable statement already allocated in the pool
		for (int index = 0; index < nStatements; index++) {
			stmt = preparedStatementsPool.get(index);
			if (stmt.isReusable(query)) {
				stmt.setStatus(ResourceStatus.BUSY);
				return stmt; // This can be used
			}
		}

		// If no one is available, create a new statement, add it to the pool
		stmt = new MyPreparedStatement(this.psDBConnection, query);
		preparedStatementsPool.add(stmt);

		stmt.setStatus(ResourceStatus.BUSY);
		return stmt; // Brand new statement ready to be used.
	}

	/**
	 * Call this method when you are done with a statement in order to return it to
	 * the pool. If the statement is <i>BUSY</i>, it will be closed. After calling
	 * this method the statement can be recycled.
	 * 
	 * @param stmt
	 *            Statement to be released.
	 * @throws SQLException
	 *             When an error occurs closing the statement.
	 */
	public void relaseStatement(MyStatement stmt) throws SQLException {

		if (stmt == null)
			throw new IllegalArgumentException("statement is null");

		if (stmt.getStatus() == ResourceStatus.BUSY) {
			stmt.close();
		}

		stmt.setStatus(ResourceStatus.RECYCLABLE);
	}

	/**
	 * Call this method when you are done with a prepared statement in order to return it to
	 * the pool. If the statement is <i>BUSY</i>, it will be closed. After calling
	 * this method the statement can be recycled (for the same query).
	 * 
	 * @param stmt
	 *            Statement to be released.
	 * @throws SQLException
	 *             When an error occurs closing the statement.
	 */
	public void relasePreparedStatement(MyPreparedStatement stmt) throws SQLException {

		if (stmt == null)
			throw new IllegalArgumentException("prepared statement is null");

		if (stmt.getStatus() == ResourceStatus.BUSY) {
			stmt.close();
		}

		stmt.setStatus(ResourceStatus.RECYCLABLE);
	}

	/**
	 * Close all resources opened against the database.
	 */
	public void close() {

		// Close statements
		statementsPool.forEach(s -> {
			try {
				s.doClose();
			} catch (SQLException e) {
				/* Ignore errors */}
			s.setStatus(ResourceStatus.UNUSED);
		});

		// Close prepared statements 
		preparedStatementsPool.forEach(s -> {
			try {
				s.doClose();
			} catch (SQLException e) {
				/* Ignore errors */}
			s.setStatus(ResourceStatus.PRESET);
		});

		// Close database connection
		if (psDBConnection != null) {
			try {
				psDBConnection.close();
			} catch (SQLException e) {
				MessageLogger.logUnmanagedException(e);
			} finally {
				psDBConnection = null;
			}
		}
	}
}