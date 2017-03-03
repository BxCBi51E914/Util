package org.tank.mysql;

/**
 * Created by tankg
 * on 2016/12/28.
 */
public class SQLConfiguration {

	public static boolean printStackTrace = true;
	public static boolean showLog = true;

	static String database = "mysql";
	static String databaseName = "test";
	static String username = "root";
	static String password = "2333";
	static int portNumber = 3306;
	static String targetIP = "192.168.13.100";
	static String characterEncoding = "utf8";
	static boolean useSSL = false;
	static boolean useUnicode = true;
	static boolean autoReconnect = true;
	static boolean failOverReadOnly = false;
	static int maxCountPerBatch = 1024;
	static int maxSize = 128;
	static int minSize = 2;
	static int initSize = 32;
	static int increment = 4;
	static long timeout = 120000;
	static long idleTestPeriod = 120000;
	static String keepAliveSQL = "SELECT 1;";
	static boolean closeIdleConnection = true;
	static long maxIdleTime = 300000;
	static int retryTimesWhileGetNullConnection = - 1;
	static long retryDurationDuringGetNullConnection = 1000;
	static int retryTimesWhileCanNotConnectServer = - 1;
	static long retryDurationDuringConnectingServer = 1000;
	static long maxUsingTime = - 1;
	static String driver = "com.mysql.jdbc.Driver";

	public static String url = "jdbc:" + database + "://"
	                    + targetIP
	                    + ":" + portNumber
	                    + "/" + databaseName
	                    + "?characterEncoding=" + characterEncoding
	                    + "&useUnicode=" + useUnicode
	                    + "&useSSL=" + useSSL
	                    + "&autoReconnect=" + autoReconnect
	                    + "&failOverReadOnly=" + failOverReadOnly;


	public static void readConfiguration() {

	}

	/**
	 * 获得执行 count 条 sql 语句时需要执行多少批次
	 *
	 * @param count sql 语句个数
	 * @return 批次数目
	 */
	public static int getBatchCount(int count) {
		return (count + maxCountPerBatch - 1) / maxCountPerBatch;
	}
}
