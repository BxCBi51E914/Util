package org.tank.mysql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Tank
 *         on 2016/12/30.
 */
class SQLCondition {
	final static String SET = " SET";

	final static String AND = " AND";
	final static String OR = " OR";

	final static String LIMIT = " LIMIT";

	final static String NOT_IN = " `*` NOT IN (#)";
	final static String IN = " `*` IN (#)";

	final static String BETWEEN_AND = " `*` BETWEEN ? AND ?";
	final static String GREATER_THAN_OR_EQUAL = " `*` >= ?";
	final static String SMALL_THAN_OR_EQUAL = " `*` <= ?";
	final static String GREATER_THAN = " `*` > ?";
	final static String SMALL_THAN = " `*` < ?";
	final static String NOT_EQUAL = " `*` != ?";
	final static String LIKE = " `*` LIKE ?";
	final static String EQUAL = " `*` = ?";

	final static String ORDER_BY = " ORDER BY `*` #";
	final static String DESC = "DESC";
	final static String ASC = "ASC";

	private List<String> mFields;
	private List<Object> mData;
	private String mOperation;

	SQLCondition() {
		mOperation = "";
		mFields = new ArrayList<String>();
		mData = new ArrayList<Object>();
	}

	public String getOperation() {
		return mOperation;
	}

	public List<Object> getData() {
		return mData;
	}

	SQLCondition setOperation(String operation) {
		mOperation = operation;
		return this;
	}

	SQLCondition addField(String field) {
		mFields.add(field);
		return this;
	}

	SQLCondition addData(Object data) {
		mData.add(data);
		return this;
	}

	SQLCondition addData(Object[] data) {
		Collections.addAll(mData, data);

		return this;
	}

	@Override
	public String toString() {
		String result = "";

		switch (mOperation) {
			case GREATER_THAN_OR_EQUAL:
			case SMALL_THAN_OR_EQUAL:
			case GREATER_THAN:
			case BETWEEN_AND:
			case SMALL_THAN:
			case NOT_EQUAL:
			case EQUAL:
			case LIKE:
				result = mOperation.replace("*", mFields.get(0));
				break;

			case AND:
			case OR:
				result = mOperation;
				break;

			case SET:
				for (String field : mFields) {
					result += " `" + field + "` = ?,";
				}

				result = mOperation + result.substring(0, result.length() - 1);
				break;

			case NOT_IN:
			case IN:
				for(Object value : mData) {
					result += "?, ";
				}

				result = result.substring(0, result.length() - 2) + " ";
				result = mOperation.replace("#", result);
				result = result.replace("*", mFields.get(0));
				break;

			case LIMIT:
				if(2 == mData.size()) {
					result = mOperation + " ?, ?";
				} else {
					result = mOperation + " ?";
				}
				break;

			case ORDER_BY:
				result = mOperation.replace("*", mFields.get(0)).replace("#", (String)mData.get(0));
				mFields.clear();
				mData.clear();
				break;
		}

		return result;
	}
}
