package util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.sqlite.SQLiteConnection;

import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhouqi on 2018/2/3.
 */
public class JsonSqlUtil {
	public static Connection getConnection() {
		Connection conn = null;
		try {
			Class.forName("org.sqlite.JDBC");
			//Class.forName("org.h2.Driver");
			conn = DriverManager.getConnection("jdbc:sqlite::memory:", "sa", "");
			//conn = DriverManager.getConnection("jdbc:h2:mem:test", "sa", "");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static JSONArray jsonSql(String sql, JSONArray... jsonArrays) throws Exception {
		JSONArray res = new JSONArray();
		Connection conn = getConnection();
		Statement st = conn.createStatement();
		for (int i = 0; i < jsonArrays.length; i++) {
			String insertSql = "";
			Set<String> head = new HashSet();
			JSONArray jsonArray = jsonArrays[i];
			for (int j = 0; j < jsonArray.size(); j++) {
				JSONObject jsonObject = jsonArray.getJSONObject(j);
				String key = "";
				String value = "";
				for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
					head.add(entry.getKey());
					key += entry.getKey() + ",";
					value += "'"+entry.getValue() + "',";
				}
				if (StringUtils.isNotEmpty(key)) {
					key = key.substring(0, key.length() - 1);
				}
				if (StringUtils.isNotEmpty(value)) {
					value = value.substring(0, value.length() - 1);
				}
				if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
					continue;
				}
				insertSql += "INSERT INTO _" + (i + 1) + "(" + key + ")" +
						"values(" + value + ");";
			}
			String columns = "";
			for (String column : head) {
				columns += "`" + column + "` varchar(10000),";
			}
			if (StringUtils.isNotEmpty(columns)) {
				columns = columns.substring(0, columns.length() - 1);
			}
			if (StringUtils.isEmpty(columns)) {
				continue;
			}
			String createSql = "CREATE TABLE _" + (i + 1) + " (" + columns + ")";
			st.execute(createSql);
			st.executeUpdate(insertSql);
			print(st.executeQuery("select * from _"+(i+1)));
		}
		ResultSet rs = st.executeQuery(sql);
		ResultSetMetaData resultSetMD = rs.getMetaData();
		for (int i = 1; i <= resultSetMD.getColumnCount(); i++) {
			System.out.print(resultSetMD.getColumnName(i) + "\t");
		}
		System.out.println();
		while (rs.next()) {
			JSONObject jsonObject = new JSONObject();
			for (int i = 1; i <= resultSetMD.getColumnCount(); i++) {
				jsonObject.put(resultSetMD.getColumnName(i), rs.getString(i));
				System.out.print(rs.getString(i) + "\t");
			}
			res.add(jsonObject);
			System.out.println();
		}
		System.out.println("------------------------------------------------------");
		return res;
	}

	public static void print(ResultSet rs) throws Exception{
		ResultSetMetaData resultSetMD = rs.getMetaData();
		for (int i = 1; i <= resultSetMD.getColumnCount(); i++) {
			System.out.print(resultSetMD.getColumnName(i) + "\t");
		}
		System.out.println();
		while (rs.next()) {
			for (int i = 1; i <= resultSetMD.getColumnCount(); i++) {
				System.out.print(rs.getString(i) + "\t");
			}
			System.out.println();
		}
		System.out.println("------------------------------------------------------");
	}

	public static void main(String[] args) throws Exception {
		String json = "[{\n" +
				"\t\t\t\t\t\"1corp_code\": \"C00001\",\n" +
				"\t\t\t\t\t\"corp_name\": \"大连\",\n" +
				"\t\t\t\t\t\"yingshoujine_ri\": \"3311795.5\"\n" +
				"\t\t\t\t},\n" +
				"\t\t\t\t{\n" +
				"\t\t\t\t\t\"1corp_code\": \"YT8888\",\n" +
				"\t\t\t\t\t\"corp_name\": \"烟台\",\n" +
				"\t\t\t\t\t\"yingshoujine_ri\": \"290010\"\n" +
				"\t\t\t\t},\n" +
				"\t\t\t\t{\n" +
				"\t\t\t\t\t\"1corp_code\": \"SM8888\",\n" +
				"\t\t\t\t\t\"corp_name\": \"厦门\",\n" +
				"\t\t\t\t\t\"yingshoujine_ri\": \"204600\"\n" +
				"\t\t\t\t},\n" +
				"\t\t\t\t{\n" +
				"\t\t\t\t\t\"1corp_code\": \"A10001\",\n" +
				"\t\t\t\t\t\"corp_name\": \"北京\",\n" +
				"\t\t\t\t\t\"yingshoujine_ri\": \"23493130.1\"\n" +
				"\t\t\t\t}]";
		String json2 = "[" +
				"\t\t\t\t{\n" +
				"\t\t\t\t\t\"corp_code\": \"SM8888\",\n" +
				"\t\t\t\t\t\"Id\": \"3\",\n" +
				"\t\t\t\t},\n" +
				"{\n" +
				"\t\t\t\t\t\"corp_code\": \"A10001\",\n" +
				"\t\t\t\t\t\"Id\": \"1\",\n" +
				"\t\t\t\t},]";
		JSONArray res = jsonSql("select * from _1 ",
				JSONArray.parseArray(json),JSONArray.parseArray(json2));
		System.out.println(res.toJSONString());
	}

	public static void main2(String[] args) throws Exception {
		Connection conn = getConnection();
		Statement st = conn.createStatement();
		st.execute("CREATE TABLE Persons\n" +
				"(\n" +
				"id int,\n" +
				"LastName varchar(255),\n" +
				"FirstName varchar(255),\n" +
				"Address varchar(255),\n" +
				"City varchar(255)\n" +
				")");
		st.executeUpdate("INSERT INTO Persons(id) VALUES(1)");
		ResultSet rs = st.executeQuery("SELECT * from Persons");
		ResultSetMetaData resultSetMD = rs.getMetaData();
		for (int i = 1; i < resultSetMD.getColumnCount(); i++) {
			System.out.print(resultSetMD.getColumnName(i) + "\t");
		}
		System.out.println();
		while (rs.next()) {
			for (int i = 1; i < resultSetMD.getColumnCount(); i++) {
				System.out.print(rs.getString(i) + "\t");
			}
			System.out.println();
		}
	}
}
