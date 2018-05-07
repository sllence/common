package util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhouqi on 2018/4/24.
 */
public class JsonParse {
	public static void main(String[] args) {
		String json = "{\n" +
				"\t\"key\": 1211212,\n" +
				"\t\"type\": \"hiveData\",\n" +
				"\t\"name\": \"链家币数据统计\",\n" +
				"\t\"city\": [{\n" +
				"\t\t\"city_name\": \"石家庄\",\n" +
				"\t\t\"city_code\": \"130100\",\n" +
				"\t\t\"corp_name\": \"石家庄链家\",\n" +
				"\t\t\"org_code\": \"SJZLJ8888\",\n" +
				"\t\t\"ophr_positive_city_id\": \"3\"\n" +
				"\t}],\n" +
				"\t\"dataSourceDesc\": {\n" +
				"\t\t\"url\": \"http://i.data.api.lianjia.com/v2/meta/merlin/lianjiabi_sum\",\n" +
				"\t\t\"param\": {\n" +
				"\t\t\t\"begin\": \"january\",\n" +
				"\t\t\t\"end\": \"endMonthLastAuto\",\n" +
				"\t\t\t\"page\": \"1\",\n" +
				"\t\t\t\"size\": \"10000\"\n" +
				"\t\t}\n" +
				"\t}\n" +
				"}";
		List<Item> parse = parse((JSON) JSON.parse(json));

		System.out.println(json);

		System.out.printf("%-35s", "key");
		System.out.printf("%-35s", "name");
		System.out.printf("%-35s", "alias");
		System.out.printf("%-20s", "type");
		System.out.printf("%-40s", "value");
		System.out.println();
		System.out.println();
		for (Item item : parse) {
			System.out.printf("%-35s", item.getKey());
			System.out.printf("%-35s", item.getName());
			System.out.printf("%-35s", item.getAlias());
			System.out.printf("%-20s", item.getType());
			System.out.printf("%-40s", item.getValue());
			System.out.println();
		}
		for (Item item : parse) {
			item.setKey(null);
			item.setValue(null);
		}
		System.out.println(JSON.toJSONString(parse));
	}

	public static List<Item> parse(JSON json) {
		List<Item> res = new ArrayList<>();
		if (json != null) {
			if (json instanceof JSONObject) {
				for (Map.Entry<String, Object> entry : ((JSONObject) json).entrySet()) {
					Item item = new Item();
					item.setKey(entry.getKey());
					item.setAlias(entry.getKey());
					item.setName(entry.getKey());
					item.setType(entry.getValue().getClass().getSimpleName());
					item.setValue(toString(entry.getValue()));
					res.add(item);
					if (entry.getValue() instanceof JSON) {
						List<Item> items = parse((JSON) entry.getValue());
						for (Item childItem : items) {
							if (childItem.getKey().startsWith("[")) {
								childItem.setKey(entry.getKey() + childItem.getKey());
							} else {
								childItem.setKey(entry.getKey() + "." + childItem.getKey());
							}
							childItem.setName(entry.getKey() + "_" + childItem.getName());
							childItem.setAlias(entry.getKey() + "_" + childItem.getAlias());
						}
						res.addAll(items);
					}
				}
			} else {
				JSONArray jsonArray = (JSONArray) json;
				for (int i = 0; i < jsonArray.size(); i++) {
					String key = "[" + i + "]";
					Object obj = jsonArray.get(i);
					Item item = new Item();
					item.setKey(key);
					item.setName("" + i);
					item.setAlias("" + i);
					item.setType(obj.getClass().getSimpleName());
					item.setValue(toString(obj));
					res.add(item);
					if (obj instanceof JSON) {
						List<Item> items = parse((JSON) obj);
						for (Item childItem : items) {
							if (childItem.getKey().startsWith("[")) {
								childItem.setKey(key + childItem.getKey());
							} else {
								childItem.setKey(key + "." + childItem.getKey());
							}
							childItem.setName(i + "_" + childItem.getName());
							childItem.setAlias(i + "_" + childItem.getAlias());
						}
						res.addAll(items);
					}
				}
			}

		}
		return res;
	}

	public static String toString(Object object) {
		return object == null ? null : object.toString();
	}
}
