package org.tank.mysql;


import entity.Word;

import java.util.List;

public class Test {

	public static void main(String[] arg) {
		// 输出当前数据库的最大连接数
//		System.out.println(SQLUtil.getMySQLMaxConnections());

		// 是否要抛出异常
//		System.out.println(SQLConfiguration.printStackTrace);

		// 是否要输出 SQL 语句以及操作的相应字段到控制台
//		System.out.println(SQLConfiguration.showLog);

//		List<Word> words = new ArrayList<Word>();
//
//		for(int i = 0; i < 10000; ++ i) {
//			Word word = new Word();
//			word.setId(20000+i);
//			word.setExplain("qwe");
//			word.setIpa("asd");
//			word.setWord("zxc");
//			words.add(word);
//		}
//
//		// 插入语句, 注意要使用事务
//		SQLUtil.insert()
//		       .insert(words)
//	           .into(Word.class)
//	           .field(Word.class)
//	           .table("dict")
//	           .useTransaction()
//	           .executeDelete();

//		// 查询数据
//		List<Word> words = SQLUtil.query()
//		                          .table(Word.class)
//		                          .table("dict")
//		                          .field("id", "explain")
//		                          .where()
//		                          .greaterThan("id", 10000)
//		                          .and()
//		                          .smallerThan("id", 20000)
//		                          .orderBy("id", false)
//		                          .limit(1000)
//		                          .findAll();
//
//		for(Word word : words) {
//			System.out.println(word);
//		}

//		// 更新数据, 多表更新的时候, 需要在 table 里面传入每个表,
//		// 并且 set 的时候表名加字段名
//		System.out.println(SQLUtil.update()
//		                          .useTransaction()
//		                          .table("dict")
//		                          .set("explain", "qwer")
//		                          .set("ipa", "asdf")
//		                          .where()
//		                          .greaterThan("id", 10000)
//		                          .and()
//		                          .smallerThan("id", 20000)
//		                          .limit(1000)
//		                          .executeUpdate());
	}
}
