package org.tank.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Tank
 * on 2017/1/8.
 */
public class SQLPool {
	private AtomNumber mCurrentSize = new AtomNumber(0);
	private Map<Connection, SQLConnection> mUsingPool = null;
	private Queue<SQLConnection> mPool = null;
	private Thread mCleanPoolThread = null;
	private boolean mKeepClean = true;
	private boolean mClosed = false;

	/**
	 * 获得当前连接池中连接的数量
	 *
	 * @return 当前连接池中连接的数量
	 */
	public int getCurrentSize() {
		return mCurrentSize.getValue();
	}

	/**
	 * 获得当前连接池中正在使用的连接的数量
	 *
	 * @return 当前连接池中正在使用的连接的数量
	 */
	public int getCurrentUsingSize() {
		return mUsingPool.size();
	}

	/**
	 * 获得当前连接池中未被使用的连接的数量
	 *
	 * @return 当前连接池中未被使用的连接的数量
	 */
	public int getCurrentFreeSize() {
		return mPool.size();
	}

	/**
	 * 判断连接池是否满了
	 *
	 * @return 连接池满了返回 true, 反之返回 false
	 */
	public boolean isFull() {
		if(getCurrentSize() + 1 == SQLConfiguration.maxSize) {
			return true;
		}

		return false;
	}

	/**
	 * 初始化连接池, 往连接池中添加 SQLConfiguration.initSize 个连接, 同时设置清理线程
	 * 需检查连接的相应数据库中设置的 maxSize, 保证该值大于 SQLConfiguration.maxSize
	 * 该方法只允许被调用一次
	 */
	public void init() {
		mUsingPool = new ConcurrentHashMap<Connection, SQLConnection>();
		mPool = new ConcurrentLinkedQueue<SQLConnection>();
		mCurrentSize.setValue(0);
		mClosed = false;

		incConnection(SQLConfiguration.initSize);

		if(null != mCleanPoolThread) {
			mKeepClean = false;
			mCleanPoolThread.interrupt();

			try {
				Thread.sleep(SQLConfiguration.idleTestPeriod);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			mKeepClean = true;
		}

		mCleanPoolThread = new Thread("SQLPool Cleaner") {
			@Override
			public void run() {
				while(mKeepClean) {
					try {
						Thread.sleep(SQLConfiguration.idleTestPeriod);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					// 清理连接池中的连接
					SQLConnection sqlConnection = null;
					int availableCount = mPool.size();
					long now;

					for(int idx = 0; idx < availableCount; ++ idx) {
						sqlConnection = mPool.poll();

						if(null == sqlConnection) {
							break;
						}

						now = Calendar.getInstance().getTimeInMillis();

						if(SQLConfiguration.closeIdleConnection
						   && sqlConnection.getLastUsingTime() + SQLConfiguration.maxIdleTime < now
						   && mCurrentSize.getValue() > SQLConfiguration.minSize) {
							try {
								sqlConnection.closeRealConnection();
								sqlConnection = null;
							} catch (SQLException e) {
								e.printStackTrace();
							}

							mCurrentSize.inc(- 1);
						} else { // 如果不需要关闭连接, 则需要保证连接可用
							makeConnectUsable(sqlConnection);
							mPool.offer(sqlConnection);
						}
					}

					// 清理长期未释放的连接
					Set<Connection> keys = mUsingPool.keySet();

					for(Connection key : keys) {
						sqlConnection = mUsingPool.get(key);
						now = Calendar.getInstance().getTimeInMillis();

						if(SQLConfiguration.maxUsingTime > 0
						   && sqlConnection.getLastUsingTime() + SQLConfiguration.maxUsingTime
						      < now) {
							try {
								sqlConnection.closeRealConnectionAndRelease();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		};

		mCleanPoolThread.start();
	}

	/**
	 * 添加 incSize 个连接到连接池中
	 * (由调用此方法的方法控制同步锁)
	 *
	 * @param incSize 增加的连接池的个数
	 */
	private void incConnection(int incSize) {
		try {
			Class.forName(SQLConfiguration.driver);

			for(int i = 0; incSize > i && ! isFull(); ++ i) {
				DriverManager.setLoginTimeout((int) (SQLConfiguration.timeout / 1000));

				try {
					Connection connection = DriverManager.getConnection(SQLConfiguration.url,
					                                                    SQLConfiguration.username,
					                                                    SQLConfiguration.password);
					releaseConnection(connection);
					mCurrentSize.inc(1);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 释放连接, 将其放回连接池
	 *
	 * @param connection 要释放的连接
	 */
	public void releaseConnection(Connection connection) {
		mUsingPool.remove(connection);

		SQLConnection sqlConnection = new SQLConnection();
		sqlConnection.setSQLPool(this);
		sqlConnection.setConnection(connection);
		sqlConnection.setLastUsingTime(Calendar.getInstance().getTimeInMillis());

		mPool.add(sqlConnection);
	}

	/**
	 * 新建一个连接
	 * 用于强制关闭长期占用的连接后保持连接池中连接池的个数
	 *
	 * @param connection 强制关闭了的连接
	 */
	public void newConnection(Connection connection) {
		mUsingPool.remove(connection);

		SQLConnection sqlConnection = new SQLConnection();
		sqlConnection.setSQLPool(this);
		sqlConnection.setLastUsingTime(Calendar.getInstance().getTimeInMillis());

		DriverManager.setLoginTimeout((int) (SQLConfiguration.timeout / 1000));

		try {
			connection = DriverManager.getConnection(SQLConfiguration.url,
			                                         SQLConfiguration.username,
			                                         SQLConfiguration.password);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		sqlConnection.setConnection(connection);

		mPool.add(sqlConnection);
	}

	/**
	 * 确保让该连接可用(调用前得保证 sqlConnection 非空)
	 *
	 * @param sqlConnection 数据库连接
	 */
	public void makeConnectUsable(SQLConnection sqlConnection) {
		int count = 0;

		while(count < SQLConfiguration.retryTimesWhileCanNotConnectServer
		      || SQLConfiguration.retryTimesWhileCanNotConnectServer <= 0) {

			try(PreparedStatement preparedStatement
					    = sqlConnection.prepareStatement(SQLConfiguration.keepAliveSQL)) {
				preparedStatement.execute();
				preparedStatement.close();
				break;
			} catch (SQLException e) {
				e.printStackTrace();
			}

			DriverManager.setLoginTimeout((int) (SQLConfiguration.timeout / 1000));

			try {
				Connection connection = DriverManager.getConnection(SQLConfiguration.url,
				                                                    SQLConfiguration.username,
				                                                    SQLConfiguration.password);
				sqlConnection.setConnection(connection);
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				Thread.sleep(SQLConfiguration.retryDurationDuringConnectingServer);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			++ count;
		}
	}

	/**
	 * 从数据库连接池中获得一个连接
	 *
	 * @return SQLConnection 或者 null
	 */
	public SQLConnection getConnection() {
		if(mClosed) {
			return null;
		}

		synchronized(this) {
			if(null == mPool) {
				init();
			}
		}

		SQLConnection sqlConnection = mPool.poll();
		int count = 0;

		while(null == sqlConnection
		      && (count < SQLConfiguration.retryTimesWhileGetNullConnection
		          || SQLConfiguration.retryTimesWhileGetNullConnection <= 0)) {
			synchronized(this) {
				if(! isFull()) {
					incConnection(SQLConfiguration.increment);
				}
			}

			sqlConnection = mPool.poll();

			if(sqlConnection != null) {
				break;
			}

			try {
				Thread.sleep(SQLConfiguration.retryDurationDuringGetNullConnection);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			++ count;
		}

		if(sqlConnection != null) {
			makeConnectUsable(sqlConnection);

			try {
				sqlConnection.setAutoCommit(true);
			} catch (SQLException e) {
				e.printStackTrace();
			}

			mUsingPool.put(sqlConnection.getConnection(), sqlConnection);
		}

		return sqlConnection;
	}

	/**
	 * 关闭连接池
	 */
	public void close() {
		mKeepClean = false;
		mClosed = true;
	}
}
