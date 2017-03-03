package org.tank.mysql;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Created by Tank
 * on 2017/1/5.
 */
public class SQLConnection implements Connection {

	private Connection mConnection = null;
	private long mLastUsingTime = 0;
	private SQLPool mPool = null;

	public void setConnection(Connection connection) {
		this.mConnection = connection;
	}

	public Connection getConnection() {
		return this.mConnection;
	}

	public long getLastUsingTime() {
		return this.mLastUsingTime;
	}

	public void setLastUsingTime(long mLastUsingTime) {
		this.mLastUsingTime = mLastUsingTime;
	}

	public void setSQLPool(SQLPool pool) {
		this.mPool = pool;
	}

	public void closeRealConnection() throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.close();
		}

		this.mConnection = null;
	}

	/**
	 * 关闭 jdbc 的真实连接, 且将其放回连接池中, 保持连接数
	 *
	 * @throws SQLException 数据库异常
	 */
	public void closeRealConnectionAndRelease() throws SQLException {
		synchronized(this) {
			// 防止 this.mConnection 被多个线程使用的情况
			Connection connection = this.mConnection;
			this.mConnection = null;

			if(connection != null) {
				connection.close();
				this.mPool.newConnection(connection);
			}
		}
	}

	@Override public void close() throws SQLException {
		synchronized(this) {
			Connection connection = mConnection;
			mConnection = null;

			if(connection != null) {
				if(! connection.getAutoCommit()) {
					connection.rollback();
				}

				mPool.releaseConnection(connection);
			}
		}
	}

	@Override public Statement createStatement() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createStatement();
		}

		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareCall(sql);
		}

		return null;
	}

	@Override public CallableStatement prepareCall(String sql) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareCall(sql);
		}

		return null;
	}

	@Override public String nativeSQL(String sql) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.nativeSQL(sql);
		}

		return null;
	}

	@Override public void setAutoCommit(boolean autoCommit) throws SQLException {
		if(this.mConnection != null && this.getAutoCommit() != autoCommit) {
			this.mConnection.setAutoCommit(autoCommit);
		}
	}

	@Override public boolean getAutoCommit() throws SQLException {
		return mConnection != null && this.mConnection.getAutoCommit();
	}

	@Override public void commit() throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.commit();
		}
	}

	public void commitIfNeed() throws SQLException {
		if(this.mConnection != null && ! this.mConnection.getAutoCommit()) {
			this.mConnection.commit();
		}
	}

	@Override public void rollback() throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.rollback();
		}
	}

	public void rollbackIfNeed() throws SQLException {
		if(this.mConnection != null && ! this.mConnection.getAutoCommit()) {
			this.mConnection.rollback();
		}
	}

	@Override public boolean isClosed() throws SQLException {
		return this.mConnection != null && this.mConnection.isClosed();

	}

	@Override public DatabaseMetaData getMetaData() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getMetaData();
		}

		return null;
	}

	@Override public void setReadOnly(boolean readOnly) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.setReadOnly(readOnly);
		}
	}

	@Override public boolean isReadOnly() throws SQLException {
		return this.mConnection != null && this.mConnection.isReadOnly();

	}

	@Override public void setCatalog(String catalog) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.setCatalog(catalog);
		}
	}

	@Override public String getCatalog() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getCatalog();
		}

		return null;
	}

	@Override public void setTransactionIsolation(int level) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.setTransactionIsolation(level);
		}
	}

	@Override public int getTransactionIsolation() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getTransactionIsolation();
		}

		return 0;
	}

	@Override public SQLWarning getWarnings() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getWarnings();
		}

		return null;
	}

	@Override public void clearWarnings() throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.clearWarnings();
		}
	}

	@Override public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createStatement(resultSetType, resultSetConcurrency);
		}

		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
	                                          int resultSetType,
	                                          int resultSetConcurrency) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
		}

		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
		}

		return null;
	}

	@Override public Map<String, Class<?>> getTypeMap() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getTypeMap();
		}

		return null;
	}

	@Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.setTypeMap(map);
		}
	}

	@Override public void setHoldability(int holdability) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.setHoldability(holdability);
		}
	}

	@Override public int getHoldability() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getHoldability();
		}

		return 0;
	}

	@Override public Savepoint setSavepoint() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.setSavepoint();
		}

		return null;
	}

	@Override public Savepoint setSavepoint(String name) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.setSavepoint(name);
		}

		return null;
	}

	@Override public void rollback(Savepoint savepoint) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.rollback();
		}
	}

	@Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.releaseSavepoint(savepoint);
		}
	}

	@Override
	public Statement createStatement(int resultSetType,
	                                 int resultSetConcurrency,
	                                 int resultSetHoldability) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createStatement(resultSetType,
			                                        resultSetConcurrency,
			                                        resultSetHoldability);
		}

		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql,
	                                          int resultSetType,
	                                          int resultSetConcurrency,
	                                          int resultSetHoldability) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareCall(sql,
			                                    resultSetType,
			                                    resultSetConcurrency,
			                                    resultSetHoldability);
		}

		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql,
	                                     int resultSetType,
	                                     int resultSetConcurrency,
	                                     int resultSetHoldability) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareCall(sql,
			                                    resultSetType,
			                                    resultSetConcurrency,
			                                    resultSetHoldability);
		}

		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareStatement(sql, autoGeneratedKeys);
		}

		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareStatement(sql, columnIndexes);
		}

		return null;
	}

	@Override public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.prepareStatement(sql, columnNames);
		}

		return null;
	}

	@Override public Clob createClob() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createClob();
		}

		return null;
	}

	@Override public Blob createBlob() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createBlob();
		}

		return null;
	}

	@Override public NClob createNClob() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createNClob();
		}

		return null;
	}

	@Override public SQLXML createSQLXML() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createSQLXML();
		}

		return null;
	}

	@Override public boolean isValid(int timeout) throws SQLException {
		return this.mConnection != null && this.mConnection.isValid(timeout);

	}

	@Override public void setClientInfo(String name, String value) throws SQLClientInfoException {
		if(this.mConnection != null) {
			this.mConnection.setClientInfo(name, value);
		}
	}

	@Override public void setClientInfo(Properties properties) throws SQLClientInfoException {
		if(this.mConnection != null) {
			this.mConnection.setClientInfo(properties);
		}
	}

	@Override public String getClientInfo(String name) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getClientInfo(name);
		}

		return null;
	}

	@Override public Properties getClientInfo() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getClientInfo();
		}

		return null;
	}

	@Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createArrayOf(typeName, elements);
		}

		return null;
	}

	@Override public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.createStruct(typeName, attributes);
		}

		return null;
	}

	@Override public void setSchema(String schema) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.setSchema(schema);
		}
	}

	@Override public String getSchema() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getSchema();
		}

		return null;
	}

	@Override public void abort(Executor executor) throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.abort(executor);
		}
	}

	@Override public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		if(this.mConnection != null) {
			this.mConnection.setNetworkTimeout(executor, milliseconds);
		}
	}

	@Override public int getNetworkTimeout() throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.getNetworkTimeout();
		}

		return 0;
	}

	@Override public <T> T unwrap(Class<T> iface) throws SQLException {
		if(this.mConnection != null) {
			return this.mConnection.unwrap(iface);
		}

		return null;
	}

	@Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return this.mConnection != null && this.mConnection.isWrapperFor(iface);
	}
}
