package com.indigoid.dbutils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.sql.PreparedStatement;
import java.sql.Ref;

public class MyPreparedStatement extends MyStatement implements PreparedStatement {

	/**
	 * The java.sql.PreparedStatement object wrapper by this instance.
	 */
	private PreparedStatement stmt;
	/**
	 * Prepared (precompiled) statements are tied to a parameterized query and can
	 * only be reused to execute the same query again, although parameters values
	 * may vary from one execution to the next.
	 */
	private String query;

	/**
	 * Creates a new prepared statement and set its status to <i>UNUSED</i>
	 * 
	 * @param con
	 *            Database manager connection
	 * @param query
	 *            Query string with parameters
	 * @throws SQLException
	 *             When thrown by the createStatement() method.
	 */
	public MyPreparedStatement(Connection con, String query) throws SQLException {
		super();
		this.query = query;
		this.stmt = con.prepareStatement(query);
		super.setStatement(stmt);
		this.setStatus(ResourceStatus.PRESET);
	}

	/**
	 * Indicates if a prepared statement can be reused to execute a parameterized
	 * query. In order to do that, the statement object must be on a non-BUSY state
	 * and it also must have been prepared (compiled) for the exact same
	 * parameterized query. 
	 * 
	 * @param query
	 *            Parameterized query for which you want to reuse this statement.
	 * @return True if this prepared statement instance can be reused.
	 */
	public boolean isReusable(String query) {
		ResourceStatus status = this.getStatus();

		if (status == ResourceStatus.PRESET || status == ResourceStatus.RECYCLABLE) {
			if (this.query.equals(query)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void addBatch() throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.addBatch();
	}

	@Override
	public void clearParameters() throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.clearParameters();
	}

	@Override
	public boolean execute() throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		return this.stmt.execute();
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		return this.stmt.executeQuery();
	}

	@Override
	public int executeUpdate() throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		return this.stmt.executeUpdate();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return this.stmt.getMetaData();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return this.stmt.getParameterMetaData();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setArray(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setAsciiStream(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBigDecimal(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBinaryStream(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBlob(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBlob(parameterIndex, inputStream);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBlob(parameterIndex, inputStream, length);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBoolean(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setByte(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setBytes(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setCharacterStream(parameterIndex, reader);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setClob(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setClob(parameterIndex, reader);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setClob(parameterIndex, reader, length);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setDate(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setDate(parameterIndex, x, cal);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setDouble(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setFloat(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setInt(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setLong(parameterIndex, x);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNCharacterStream(parameterIndex, value);

	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNCharacterStream(parameterIndex, value);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNClob(parameterIndex, value);

	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNClob(parameterIndex, reader);

	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNClob(parameterIndex, reader, length);
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNString(parameterIndex, value);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNull(parameterIndex, sqlType);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setNull(parameterIndex, sqlType, typeName);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setObject(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setObject(parameterIndex, x, targetSqlType);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setRef(parameterIndex, x);
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setRowId(parameterIndex, x);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setSQLXML(parameterIndex, xmlObject);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setShort(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setString(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setTime(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setTime(parameterIndex, x, cal);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setTimestamp(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setTimestamp(parameterIndex, x, cal);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setURL(parameterIndex, x);
	}

	@Override
	@Deprecated
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		this.setStatus(ResourceStatus.BUSY);
		this.stmt.setUnicodeStream(parameterIndex, x, length);
	}
}