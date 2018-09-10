package com.indigoid.dbutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * This class implements the Statement interface. <br/>
 * <br/>
 * It is a wrapper for a standard <strong>java.sql.Statement</strong> with the
 * addition of a status field (<strong>ResourceStatus</strong>). The status
 * filed allows to create a pool of statements and to recycle those that has
 * already being used. Statements used with <i>INSERT, UPDATE or DELETE</i>
 * statements are automatically recyclable after the execution of the statement.
 * Statements with queries are kept as <i>BUSY</i> up until the
 * <strong>close()</strong> or <strong>cancel()</strong> method is invoked.
 * <br/>
 * <br/>
 * A call to a setter method will result on the status of the statement to
 * become <i>BUSY</i>, although there is no result set yet (but it is being used
 * by a consumer).<br/>
 * <br/>
 * When the constructor is called, a new statements is created from the database
 * connection and this instance set as <i>UNUSED</i>.
 * 
 * @author Charlie
 *
 */
public class MyStatement implements Statement {

	/**
	 * The java.sql.Statement object wrapper by this instance.
	 */
	private Statement stmt;
	/**
	 * Status of the resource wrapped (statement).
	 */
	private ResourceStatus status;

	/**
	 * Default constructor
	 */
	protected MyStatement() {
		this.stmt = null;
	}

	/**
	 * Creates a new statement and set its status to <i>UNUSED</i>
	 * 
	 * @param con
	 *            Database manager connection
	 * @throws SQLException
	 *             When thrown by the createStatement() method.
	 */
	public MyStatement(Connection con) throws SQLException {
		this.stmt = con.createStatement();
		this.status = ResourceStatus.UNUSED;
	}

	/**
	 * @return Current statement status.
	 */
	public ResourceStatus getStatus() {
		return status;
	}

	/**
	 * Set the status for this instance.
	 * 
	 * @param status
	 *            New status.
	 */
	protected void setStatus(ResourceStatus status) {
		this.status = status;
	}

	/**
	 * This method must be called by child classes using the default constructor.
	 * Typically this classes are creating a specialized version of the statement
	 * and once created it must be passed to the parent in order for the methods on
	 * this class to work.
	 * 
	 * @param stmt
	 *            Statement created by the child class.
	 */
	protected void setStatement(Statement stmt) {
		this.stmt = stmt;
	}

	/**
	 * @return <strong>true</strong> if this instance can be reused.
	 */
	public boolean isUsable() {
		return (this.status == ResourceStatus.UNUSED || this.status == ResourceStatus.RECYCLABLE);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return this.stmt.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return this.stmt.unwrap(iface);
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.addBatch(sql);
	}

	@Override
	public void cancel() throws SQLException {
		this.stmt.cancel();
		this.status = ResourceStatus.RECYCLABLE;
	}

	@Override
	public void clearBatch() throws SQLException {
		this.stmt.clearBatch();
	}

	@Override
	public void clearWarnings() throws SQLException {
		this.stmt.clearWarnings();
	}

	@Override
	public void close() throws SQLException {
		
		// If there is a result set associated to the statement close it
		// but the statement itself is not closed.
		ResultSet rs = this.getResultSet();
		if (rs != null) {
			rs.close();			
		}
		
		this.status = ResourceStatus.RECYCLABLE;
	}
	
	/**
	 * This method is called to really close the statement upon program 
	 * completion. 
	 * @throws SQLException When thrown by the java.sql.Stetemnt close() method
	 */
	public void doClose() throws SQLException {
		this.stmt.close();		
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.closeOnCompletion();
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		this.status = ResourceStatus.BUSY;
		return this.execute(sql);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		this.status = ResourceStatus.BUSY;
		return this.execute(sql, autoGeneratedKeys);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		this.status = ResourceStatus.BUSY;
		return this.execute(sql, columnIndexes);
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		this.status = ResourceStatus.BUSY;
		return this.stmt.execute(sql, columnNames);
	}

	@Override
	public int[] executeBatch() throws SQLException {
		this.status = ResourceStatus.BUSY;
		return this.executeBatch();
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		this.status = ResourceStatus.BUSY;
		return this.stmt.executeQuery(sql);
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		this.status = ResourceStatus.RECYCLABLE;
		return this.stmt.executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		this.status = ResourceStatus.RECYCLABLE;
		return this.stmt.executeUpdate(sql, autoGeneratedKeys);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		this.status = ResourceStatus.RECYCLABLE;
		return this.stmt.executeUpdate(sql, columnIndexes);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		this.status = ResourceStatus.RECYCLABLE;
		return this.executeUpdate(sql, columnNames);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.stmt.getConnection();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return this.stmt.getFetchDirection();
	}

	@Override
	public int getFetchSize() throws SQLException {
		return this.stmt.getFetchSize();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return this.stmt.getGeneratedKeys();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return this.stmt.getMaxFieldSize();
	}

	@Override
	public int getMaxRows() throws SQLException {
		return this.stmt.getMaxRows();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return this.stmt.getMoreResults();
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return this.stmt.getMoreResults(current);
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return this.stmt.getQueryTimeout();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return this.stmt.getResultSet();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return this.stmt.getResultSetConcurrency();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return this.stmt.getResultSetHoldability();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return this.stmt.getResultSetType();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return this.stmt.getUpdateCount();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return this.stmt.getWarnings();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return this.stmt.isCloseOnCompletion();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.stmt.isClosed();
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return this.stmt.isPoolable();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setCursorName(name);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setEscapeProcessing(enable);
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setFetchDirection(direction);
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setFetchSize(rows);
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setMaxFieldSize(max);
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setMaxRows(max);
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setPoolable(poolable);
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		this.status = ResourceStatus.BUSY;
		this.stmt.setQueryTimeout(seconds);
	}
}