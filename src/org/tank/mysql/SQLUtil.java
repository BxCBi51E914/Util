package org.tank.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by Tank
 * on 2017/2/21.
 */
public class SQLUtil {

	private static SQLPool mSQLPool = new SQLPool();

	/**
	 * 创建更新命令
	 *
	 * @param <T> 查询语句返回类型
	 * @return 更新命令
	 */
	public static <T> SQLCommand update() {
		return new SQLCommand<T>().update();
	}

	/**
	 * 创建插入命令
	 *
	 * @param <T> 查询语句返回类型
	 * @return 更新命令
	 */
	public static <T> SQLCommand insert() {
		return new SQLCommand<T>();
	}

	/**
	 * 创建查询命令
	 *
	 * @param <T> 查询语句返回类型
	 * @return 更新命令
	 */
	public static <T> SQLCommand query() {
		return new SQLCommand<T>().query();
	}

	/**
	 * 创建删除命令
	 *
	 * @param <T> 查询语句返回类型
	 * @return 更新命令
	 */
	public static <T> SQLCommand delete() {
		return new SQLCommand<T>().delete();
	}

	/**
	 * 通过反射将数据转化成相应实体
	 *
	 * @param clazz 对应实体的类型
	 * @param data  数据的 HashMap, key 为变量名, value 为相应值
	 * @param <T>   对应实体类型
	 * @return 相应的实体
	 */
	private static <T> T convertDataToEntity(Class<T> clazz, HashMap<String, Object> data) {
		Reflect entity = Reflect.on(clazz).create();

		for(Map.Entry<String, Object> entry : data.entrySet()) {
			entity.set(entry.getKey(), entry.getValue());
		}

		return entity.get();
	}

	/**
	 * 将一个实体转化成一个 List<Object> 对象
	 *
	 * @param entity 相应实体
	 * @param fields 给定每个变量名, 保证填充顺序不会错
	 * @param <T>    相应实体的类型
	 * @return 该实体对应的 List<Object> 对象
	 */
	public static <T> List<Object> convertEntityToData(T entity, List<String> fields) {
		List<Object> result = new ArrayList<Object>();
		Reflect reflect = Reflect.on(entity);

		for(String field : fields) {
			result.add(reflect.field(field).get());
		}

		return result;
	}

	/**
	 * 通过反射将数据转化成相应实体
	 *
	 * @param clazz 对应实体的类型
	 * @param data  数据的 HashMap 的列表, key 为变量名, value 为相应值
	 * @param <T>   对应实体类型
	 * @return 相应的实体
	 */
	private static <T> List<T> convertDataToEntities(Class<T> clazz,
	                                                 List<HashMap<String, Object>> data) {
		List<T> result = new ArrayList<T>();

		for(HashMap<String, Object> entityData : data) {
			result.add(convertDataToEntity(clazz, entityData));
		}

		return result;
	}

	/**
	 * 输出 sql 命令
	 */
	private static void outputSQLCommand(PreparedStatement preparedStatement) {
		if(SQLConfiguration.showLog) {
			String sqlCmd = preparedStatement.toString();
			System.out.println("* SQLCommand : " + sqlCmd.substring(sqlCmd.indexOf(":") + 2));
		}
	}

	/**
	 * 输出查询到的数据每行的名称
	 */
	private static void outputColumnFields(List<String> fields) {
		if(SQLConfiguration.showLog) {
			String columnFields = "* Fields : ";

			for(String field : fields) {
				columnFields += field + ", ";
			}

			System.out.println(columnFields.substring(0, columnFields.length() - 2));
		}
	}

	/**
	 * 填充数据到 preparedStatement 中
	 *
	 * @param preparedStatement 相应 PrepareStatement
	 * @param data              数据
	 */
	private static void fillDataIntoPreparedStatement(PreparedStatement preparedStatement,
	                                                  List<Object> data) throws SQLException {
		for(int i = 0; i < data.size(); ++ i) {
			preparedStatement.setObject(i + 1, data.get(i));
		}

		outputSQLCommand(preparedStatement);
	}

	/**
	 * 填充数据到 sql 语句中, 防止 SQL 注入攻击, 按批次填充数据
	 *
	 * @param data 数据
	 */
	private static void fillDataIntoPreparedStatementWithBatch(PreparedStatement preparedStatement,
	                                                           List<List<Object>> data)
			throws SQLException {
		for(List<Object> objs : data) {
			for(int idx = 0; idx < objs.size(); ++ idx) {
				preparedStatement.setObject(idx + 1, objs.get(idx));
			}

			outputSQLCommand(preparedStatement);
			preparedStatement.addBatch();
		}
	}

	/**
	 * 从 ResultSet 中获取数据
	 *
	 * @param resultSet 相应结果集
	 * @return 以 List 类型返回结果集中的数据, 每行数据为一个 HashMap, 以键值对的形式存储每个字段的值
	 */
	private static List<HashMap<String, Object>> readDataInResultSet(ResultSet resultSet)
			throws SQLException {
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		int columnCount = resultSetMetaData.getColumnCount();
		List<String> fields = new ArrayList<String>();

		for(int i = 1; i <= columnCount; ++ i) {
			fields.add(resultSetMetaData.getColumnName(i));
		}

		outputColumnFields(fields);

		List<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> data = null;

		while(resultSet.next()) {
			data = new HashMap<String, Object>();

			for(String filed : fields) {
				data.put(filed, resultSet.getObject(filed));
			}

			result.add(data);
		}

		return result;
	}

	/**
	 * 关闭所有
	 */
	private static void releaseAll(SQLConnection connection,
	                               PreparedStatement preparedStatement,
	                               ResultSet resultSet) throws SQLException {
		if(null != connection) {
			connection.close();
		}

		if(null != preparedStatement) {
			preparedStatement.close();
		}

		if(null != resultSet) {
			resultSet.close();
		}
	}

	/**
	 * 获得 MySQL 的最大连接数
	 *
	 * @return MySQL 的最大连接数, -1 表示出错
	 */
	public static int getMySQLMaxConnections() {
		PreparedStatement preparedStatement = null;
		SQLConnection connection = null;
		ResultSet resultSet = null;
		int result = - 1;

		try {
			String sql = "show variables like ?;";
			connection = mSQLPool.getConnection();

			if(null == connection) {
				return - 1;
			}

			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, "%max_connections%");
			outputSQLCommand(preparedStatement);
			resultSet = preparedStatement.executeQuery();
			List<HashMap<String, Object>> resultData = readDataInResultSet(resultSet);

			if(null == resultData || resultData.isEmpty()) {
				result = - 1;
			} else {
				result = Integer.parseInt((String) resultData.get(0).get("Value"));
			}
		} catch (SQLException e) {
			if(SQLConfiguration.printStackTrace) {
				e.printStackTrace();
			}
		} finally {
			try {
				releaseAll(connection, preparedStatement, resultSet);
			} catch (SQLException e) {
				if(SQLConfiguration.printStackTrace) {
					e.printStackTrace();
				}
			}
		}

		return result;
	}

	/**
	 * 查询操作
	 *
	 * @param clazz 查询到的数据对应实体的类型
	 * @param sql   sql 命令
	 * @param data  相应参数
	 * @return 相应查询到的数据对应的实体
	 */
	public static <T> List<T> query(Class<T> clazz, String sql, Object... data) {
		List<Object> d = new ArrayList<Object>();

		Collections.addAll(d, data);

		return query(clazz, sql, d);
	}

	/**
	 * 执行查询操作
	 *
	 * @param clazz 查询到的数据对应实体的类型
	 * @param sql   sql 命令
	 * @param data  数据
	 * @param <T>   查询到的数据对应实体的类型
	 * @return 相应的实体的 List
	 */
	static <T> List<T> query(Class<T> clazz, String sql, List<Object> data) {
		PreparedStatement preparedStatement = null;
		SQLConnection connection = null;
		ResultSet resultSet = null;
		List<T> result = null;

		try {
			connection = mSQLPool.getConnection();

			if(null == connection) {
				return null;
			}

			preparedStatement = connection.prepareStatement(sql);

			fillDataIntoPreparedStatement(preparedStatement, data);

			resultSet = preparedStatement.executeQuery();
			List<HashMap<String, Object>> resultData = readDataInResultSet(resultSet);
			result = convertDataToEntities(clazz, resultData);
		} catch (SQLException e) {
			if(SQLConfiguration.printStackTrace) {
				e.printStackTrace();
			}
		} finally {
			try {
				releaseAll(connection, preparedStatement, resultSet);
			} catch (SQLException e) {
				if(SQLConfiguration.printStackTrace) {
					e.printStackTrace();
				}
			}
		}

		return result;
	}

//	public static <T> List<Object> entityToObjectList(List<String> fields, ) {
//
//	}
//	/**
//	 * 将相应实体拆分成一个个属性, 存入 mData.
//	 *
//	 * @param entity 要插入的实体
//	 */
//	private void insertEntityToData(T entity) {
//		if(mFields.isEmpty()) {
//			Field[] fields = entity.getClass().getDeclaredFields();
//
//			for(Field field : fields) {
//				mFields.add(field.getName());
//			}
//		}
//
//		Reflect reflect = Reflect.on(entity);
//
//		for(String field : mFields) {
//			mData.add(reflect.field(field).get());
//		}
//	}

	/**
	 * 插入 count 行数据, entities 里面存储所有数据, 每行有 (entities.size() / count) 个数据
	 *
	 * @param sql      sql 命令
	 * @param fields   要插入的数据对应的字段名, 即变量名
	 * @param entities 相应数据
	 * @return 受影响行数
	 */
	public static <T> int insert(Boolean useTransaction,
	                             String sql,
	                             List<String> fields,
	                             List<T> entities) {
		PreparedStatement preparedStatement = null;
		SQLConnection connection = null;
		int result = 0;

		try {
			connection = mSQLPool.getConnection();

			if(null == connection) {
				return 0;
			}

			if(useTransaction) {
				connection.setAutoCommit(false);
			}

			preparedStatement = connection.prepareStatement(sql);

			int[] tmpResult;
			List<List<Object>> data;
			int batchCount = SQLConfiguration.getBatchCount(entities.size());

			for(int idx = 0; idx < batchCount; ++ idx) {
				data = new ArrayList<List<Object>>();

				for(int idx1 = 0;
				    idx1 < SQLConfiguration.maxCountPerBatch
				    && idx * SQLConfiguration.maxCountPerBatch + idx1 < entities.size();
				    ++ idx1) {
					data.add(convertEntityToData(entities.get(idx
					                                          * SQLConfiguration.maxCountPerBatch
					                                          + idx1),
					                             fields));
				}

				fillDataIntoPreparedStatementWithBatch(preparedStatement, data);

				tmpResult = preparedStatement.executeBatch();
				preparedStatement.clearBatch();

				for(int tmp : tmpResult) {
					result += tmp;
				}
			}

			connection.commitIfNeed();
		} catch (SQLException e) {
			try {
				connection.rollbackIfNeed();
			} catch (SQLException e1) {
				if(SQLConfiguration.printStackTrace) {
					e1.printStackTrace();
				}
			}

			if(SQLConfiguration.printStackTrace) {
				e.printStackTrace();
			}
		} finally {
			try {
				releaseAll(connection, preparedStatement, null);
			} catch (SQLException e) {
				if(SQLConfiguration.printStackTrace) {
					e.printStackTrace();
				}
			}
		}

		return result;
	}

	/**
	 * 执行非查询操作, update, delete 和 insert
	 *
	 * @param sql  sql 命令
	 * @param data 相应参数
	 * @return 受影响行数
	 */
	public static int noQuery(Boolean useTransaction, String sql, Object... data) {
		List<Object> d = new ArrayList<Object>();

		Collections.addAll(d, data);

		return noQuery(useTransaction, sql, d);
	}

	/**
	 * 执行非查询语句
	 *
	 * @param sql  相应 sql 命令
	 * @param data 相应数据
	 * @return 受影响行数
	 */
	static int noQuery(Boolean useTransaction, String sql, List<Object> data) {
		PreparedStatement preparedStatement = null;
		SQLConnection connection = null;
		int result = 0;

		try {
			connection = mSQLPool.getConnection();

			if(null == connection) {
				return 0;
			}

			if(useTransaction) {
				connection.setAutoCommit(false);
			}

			preparedStatement = connection.prepareStatement(sql);

			fillDataIntoPreparedStatement(preparedStatement, data);
			result = preparedStatement.executeUpdate();

			connection.commitIfNeed();
		} catch (SQLException e) {
			try {
				connection.rollbackIfNeed();
			} catch (SQLException e1) {
				if(SQLConfiguration.printStackTrace) {
					e1.printStackTrace();
				}
			}

			if(SQLConfiguration.printStackTrace) {
				e.printStackTrace();
			}
		} finally {
			if(null != connection) {
				try {
					connection.setAutoCommit(false);
				} catch (SQLException e) {
					if(SQLConfiguration.printStackTrace) {
						e.printStackTrace();
					}
				}
			}

			try {
				releaseAll(connection, preparedStatement, null);
			} catch (SQLException e) {
				if(SQLConfiguration.printStackTrace) {
					e.printStackTrace();
				}
			}
		}

		return result;
	}
}
