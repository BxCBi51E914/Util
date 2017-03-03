package org.tank.mysql;

import java.util.*;

/**
 * Created by tankg
 * on 2016/12/28.
 */
public class SQLCommand<T> {
	private final static String UPDATE = "update";
	private final static String INSERT = "insert";
	private final static String DELETE = "delete";
	private final static String QUERY = "select";

	// 操作类型, 如 insert, update 之类
	private String mAction;

	// 表名, 默认为操作的类名, 也可自行指定
	private List<String> mTables;

	// 操作的字段, 比如插入的时候, 此时就是相应实体的所有属性,
	// 或者是要获取的字段, 比如查询的时候要获取的字段列表
	private List<String> mFields;

	// 条件
	private Queue<SQLCondition> mCondition;

	// 查询返回实体的类型, 可自行指定
	private Class<T> mReturnType;

	// sql 语句中的的数据
	private List<Object> mData;

	// 执行插入语句要插入的实体
	private List<T> mEnetities;

	// set 条件比较特殊, 需要将所有的 set 放在同一个条件中,
	// 这边记录一下是否已经有 set 条件
	private SQLCondition mSetOption;

	// 判断 sql 命令中是否有 table 这个关键词
	private boolean mHasWhere;

	private boolean mUseTransaction;

	public SQLCommand() {
		mCondition = new LinkedList<SQLCondition>();
		mTables = new ArrayList<String>();
		mFields = new ArrayList<String>();
		mData = new ArrayList<Object>();
		mEnetities = new ArrayList<T>();
		mUseTransaction = false;
		mReturnType = null;
		mSetOption = null;
		mHasWhere = false;
		mAction = "";
	}

	/**
	 * 更新操作
	 *
	 * @return this
	 */
	public SQLCommand update() {
		mAction = UPDATE;
		return this;
	}

	/**
	 * 插入一个实体
	 *
	 * @param entity 实体
	 * @return this
	 */
	public SQLCommand insert(T entity) {
		mAction = INSERT;
		mEnetities.add(entity);
		return this;
	}

	/**
	 * 插入多个实体
	 *
	 * @param entities 实体
	 * @return this
	 */
	public SQLCommand insert(T... entities) {
		mAction = INSERT;
		Collections.addAll(mEnetities, entities);
		return this;
	}

	/**
	 * 插入多个实体
	 *
	 * @param entities 实体
	 * @return this
	 */
	public SQLCommand insert(List<T> entities) {
		mAction = INSERT;

		for(T entity : entities) {
			mEnetities.add(entity);
		}

		return this;
	}

	/**
	 * 设置插入数据到指定类型对应名称的表
	 *
	 * @param clazz 相应类型
	 * @return this
	 */
	public SQLCommand into(Class<T> clazz) {
		mTables.add(clazz.getSimpleName());
		return this;
	}

	/**
	 * 设置插入数据到相应表
	 *
	 * @param table 相应表名
	 * @return this
	 */
	public SQLCommand into(String table) {
		mTables.add(table);
		return this;
	}

	/**
	 * 插入所有字段, 清空之前设置要插入的字段
	 *
	 * @return this
	 */
	public SQLCommand field(Class<T> clazz) {
		Reflect reflect = Reflect.on(clazz).create();
		mFields.clear();

		for(String field : reflect.fields().keySet()) {
			mFields.add(field);
		}

		return this;
	}

	/**
	 * 插入相应字段, 清空之前设置要插入的字段
	 *
	 * @param fields 字段列表
	 * @return
	 */
	public SQLCommand field(String... fields) {
		List<String> fs = new ArrayList<String>();
		Collections.addAll(fs, fields);
		return field(fs);
	}

	/**
	 * 插入相应字段, 清空之前设置要插入的字段
	 *
	 * @param fields 字段列表
	 * @return
	 */
	public SQLCommand field(List<String> fields) {
		mFields.clear();

		for(String field : fields) {
			mFields.add(field);
		}

		return this;
	}

	/**
	 * 没啥鸟用, 但是有时候用了会觉得爽
	 *
	 * @return this
	 */
	public SQLCommand where() {
		mHasWhere = true;
		return this;
	}

	/**
	 * 删除操作
	 *
	 * @return this
	 */
	public SQLCommand delete() {
		mAction = DELETE;
		return this;
	}

	/**
	 * 查询操作
	 *
	 * @return this
	 */
	public SQLCommand query() {
		mAction = QUERY;
		return this;
	}

	/**
	 * 指定表名, 会清空之前设置的所有表名
	 *
	 * @param table 相应表名
	 * @return this
	 */
	public SQLCommand table(String table) {
		mTables.clear();
		mTables.add(table);
		return this;
	}

	/**
	 * 设置操作的表的表名和查询返回的类型为相应的类名
	 * 如果表名已指定, 则保持原样; 如果查询返回的类型已指定, 同样保持原样
	 *
	 * @param clazz 相应类
	 * @return this
	 */
	public SQLCommand table(Class<T> clazz) {
		if(mTables.isEmpty()) {
			mTables.add(clazz.getSimpleName());
		}

		if(mReturnType == null) {
			mReturnType = clazz;
		}
		return this;
	}

	/**
	 * 指定多张表
	 *
	 * @param tables 表名列表
	 * @return this
	 */
	public SQLCommand tables(String... tables) {
		mTables.clear();

		for(String table : tables) {
			mTables.add(table);
		}

		return this;
	}

	/**
	 * 添加查询所需要的字段名，默认获取全部字段
	 *
	 * @param field 字段名
	 * @return this
	 */
	public SQLCommand getField(String field) {
		mFields.add(field);
		return this;
	}

	/**
	 * 添加查询条件(=)
	 *
	 * @param field 字段名
	 * @param value 字段值
	 * @return this
	 */
	public SQLCommand equalTo(String field, Object value) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.EQUAL)
		                                 .addField(field)
		                                 .addData(value));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加查询条件(!=)
	 *
	 * @param field 字段名
	 * @param value 字段值
	 * @return this
	 */
	public SQLCommand notEqual(String field, Object value) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.NOT_EQUAL)
		                                 .addField(field)
		                                 .addData(value));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加查询条件(>)
	 *
	 * @param field 字段名
	 * @param value 字段值
	 * @return this
	 */
	public SQLCommand greaterThan(String field, Object value) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.GREATER_THAN)
		                                 .addField(field)
		                                 .addData(value));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加查询条件(>=)
	 *
	 * @param field 字段名
	 * @param value 字段值
	 * @return this
	 */
	public SQLCommand greaterThanOrEqualTo(String field, Object value) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.GREATER_THAN_OR_EQUAL)
		                                 .addField(field)
		                                 .addData(value));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加查询条件(<)
	 *
	 * @param field 字段名
	 * @param value 字段值
	 * @return this
	 */
	public SQLCommand smallerThan(String field, Object value) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.SMALL_THAN)
		                                 .addField(field)
		                                 .addData(value));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加查询条件(<=)
	 *
	 * @param field 字段名
	 * @param value 字段值
	 * @return this
	 */
	public SQLCommand smallerThanOrEqualTo(String field, Object value) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.SMALL_THAN_OR_EQUAL)
		                                 .addField(field)
		                                 .addData(value));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加一个或的条件
	 *
	 * @return this
	 */
	public SQLCommand or() {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.OR));
		return this;
	}

	/**
	 * 添加一个且的条件
	 *
	 * @return this
	 */
	public SQLCommand and() {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.AND));
		return this;
	}

	/**
	 * 添加一个 IN 的条件
	 *
	 * @param field  字段名
	 * @param values 字段值
	 * @return this
	 */
	public SQLCommand in(String field, Object... values) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.IN)
		                                 .addField(field)
		                                 .addData(values));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加一个 NOT IN 的条件
	 *
	 * @param field  字段名
	 * @param values 字段值
	 * @return this
	 */
	public SQLCommand notIn(String field, Object... values) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.NOT_IN)
		                                 .addField(field)
		                                 .addData(values));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加一个 between and 条件
	 *
	 * @param field 字段名
	 * @param lhs   左边的值
	 * @param rhs   右边的值
	 * @return this
	 */
	public SQLCommand betweenAnd(String field, Object lhs, Object rhs) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.BETWEEN_AND)
		                                 .addField(field)
		                                 .addData(lhs)
		                                 .addData(rhs));
		mHasWhere = true;
		return this;
	}

	/**
	 * 添加一个 limit 条件, 分页操作, 该条件应该加在最后面
	 *
	 * @param startIndex 开始下标
	 * @param endIndex   结束下标
	 * @return this
	 */
	public SQLCommand limit(int startIndex, int endIndex) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.LIMIT)
		                                 .addData(startIndex)
		                                 .addData(endIndex));
		return this;
	}

	/**
	 * 添加一个 limit 条件, 限制影响的最大行数, 该条件应该加在最后面
	 *
	 * @param count 最大受影响行数
	 * @return this
	 */
	public SQLCommand limit(int count) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.LIMIT)
		                                 .addData(count));
		return this;
	}

	/**
	 * 按照指定字段排序
	 *
	 * @param field 字段名
	 * @param isASC 为 True 时从小到大排序, False 反之
	 * @return this
	 */
	public SQLCommand orderBy(String field, boolean isASC) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.ORDER_BY)
		                                 .addField(field)
		                                 .addData(isASC ? SQLCondition.ASC : SQLCondition.DESC));
		return this;
	}

	/**
	 * 添加一个 like 条件
	 *
	 * @param field 字段名
	 * @param regex 相应的匹配的条件
	 * @return this
	 */
	public SQLCommand like(String field, String regex) {
		mCondition.add(new SQLCondition().setOperation(SQLCondition.LIKE)
		                                 .addField(field)
		                                 .addData(regex));
		mHasWhere = true;
		return this;
	}

	/**
	 * 设置相应字段的值
	 *
	 * @param field 字段名
	 * @param value 字段值
	 * @return this
	 */
	public SQLCommand set(String field, Object value) {
		if(null == mSetOption) {
			mSetOption = new SQLCondition().setOperation(SQLCondition.SET)
			                               .addField(field)
			                               .addData(value);
			mCondition.add(mSetOption);
		} else {
			mSetOption.addField(field)
			          .addData(value);
		}

		return this;
	}

	/**
	 * 修改查询返回的实体的类型
	 *
	 * @param clazz 返回的实体的相应类型
	 * @return this
	 */
	public SQLCommand returnType(Class<T> clazz) {
		mReturnType = clazz;
		return this;
	}

	public String getSQLCommand() {
		String sql = "", tmp;
		int count;

		switch(mAction) {
			case UPDATE:
				sql = "UPDATE";

				for(String table : mTables) {
					sql += " `" + table + "`,";
				}

				sql = sql.substring(0, sql.length() - 1);

				// 默认第一个条件是 set 条件
				sql += mCondition.peek().toString();

				for(Object data : mCondition.poll().getData()) {
					mData.add(data);
				}

				if(mHasWhere) {
					sql += " WHERE";
				}

				while(! mCondition.isEmpty()) {
					sql += mCondition.peek().toString();

					for(Object data : mCondition.poll().getData()) {
						mData.add(data);
					}
				}

				sql += ";";
				break;

			case DELETE:
				sql = "DELETE FROM";

				for(String table : mTables) {
					sql += " `" + table + "`,";
				}

				sql = sql.substring(0, sql.length() - 1);

				if(mHasWhere) {
					sql += " WHERE";
				}

				while(! mCondition.isEmpty()) {
					sql += mCondition.peek().toString();

					for(Object data : mCondition.poll().getData()) {
						mData.add(data);
					}
				}

				sql += ";";
				break;

			case QUERY:
				sql = "SELECT";

				if(mFields.isEmpty()) {
					mFields.add("*");
				}

				for(String field : mFields) {
					sql += " `" + field + "`,";
				}

				sql = sql.substring(0, sql.length() - 1) + " FROM";

				for(String table : mTables) {
					sql += " `" + table + "`,";
				}

				sql = sql.substring(0, sql.length() - 1);

				if(mHasWhere) {
					sql += " WHERE";
				}

				while(! mCondition.isEmpty()) {
					sql += mCondition.peek().toString();

					for(Object data : mCondition.poll().getData()) {
						mData.add(data);
					}
				}

				sql += ";";
				break;

			case INSERT:
				sql = "INSERT INTO `" + mTables.get(0) + "` (";
				tmp = "VALUES (";

				for(String field : mFields) {
					sql += "`" + field + "`, ";
					tmp += "?, ";
				}

				sql = sql.substring(0, sql.length() - 2) + ") ";
				tmp = tmp.substring(0, tmp.length() - 2) + ");";
				sql = sql + tmp;
				break;
		}

		return sql;
	}

	/**
	 * 使用事务
	 *
	 * @return this
	 */
	public SQLCommand<T> useTransaction() {
		mUseTransaction = true;
		return this;
	}

	/**
	 * 执行更新操作
	 *
	 * @return 返回受影响行数
	 */
	public int executeUpdate() {
		return SQLUtil.noQuery(mUseTransaction, getSQLCommand(), mData);
	}

	/**
	 * 执行插入操作
	 *
	 * @return 返回受影响行数
	 */
	public int executeInsert() {
		return SQLUtil.insert(mUseTransaction, getSQLCommand(), mFields, mEnetities);
	}

	/**
	 * 执行删除操作
	 *
	 * @return 返回受影响行数
	 */
	public int executeDelete() {
		return executeNoQuery();
	}

	/**
	 * 执行查询操作
	 *
	 * @return 返回相应实体
	 */
	public List<T> executeQuery() {
		return SQLUtil.query(mReturnType, getSQLCommand(), mData);
	}

	/**
	 * 执行非查询操作
	 *
	 * @return 返回受影响行数
	 */
	public int executeNoQuery() {
		switch(mAction) {
			case UPDATE:
				return executeUpdate();

			case INSERT:
				return executeInsert();

			case DELETE:
				return executeDelete();
		}

		return 0;
	}

	/**
	 * 获取查询到的所有数据
	 *
	 * @return 获取要查询的数据
	 */
	public List<T> findAll() {
		return executeQuery();
	}

	/**
	 * 获取查询到的第一行数据
	 *
	 * @return 相应实体
	 */
	public T findFirst() {
		return executeQuery().get(0);
	}
}