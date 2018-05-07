package main;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;


public class ApiUtil {

	private static String md5(String str) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes("utf-8"));
			byte[] byteDigest = md.digest();
			int i;
			StringBuffer buf = new StringBuffer("");
			for (int offset = 0; offset < byteDigest.length; offset++) {
				i = byteDigest[offset];
				if (i < 0)
					i += 256;
				if (i < 16)
					buf.append("0");
				buf.append(Integer.toHexString(i));
			}
			// 32位加密
			return buf.toString();
			// 16位的加密
			// return buf.toString().substring(8, 24);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

	/**
	 * 获取sign
	 *
	 * @param params
	 * @param secret
	 * @return
	 */
	private static String getApiSign(Map<String, String> params, String secret) {
		List<String> list = new ArrayList<String>();
		Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
			String key = null;
			String val = null;
			try {
				key = URLEncoder.encode(entry.getKey().toString(), "utf8");
				val = URLEncoder.encode(entry.getValue().toString(), "utf8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			list.add(key + "=" + val);//(1)connecting all parameters
		}
		Collections.sort(list);//(2)sort all strings
		//System.out.println(list);
		StringBuilder buf = new StringBuilder();
		int length = list.size();
		for (int i = 0; i < length; i++) {
			buf.append(list.get(i));
			if (i < length - 1) {
				buf.append("&");
			}
		}
		//urlCode = URLEncoder.encode(buf.toString(),"utf8").toLowerCase();
		String urlCode = buf.toString().toLowerCase();
		//System.out.println(urlCode);
		String md5Str = md5(urlCode);
		//(3)append secret key
		return md5(md5Str + secret);
	}

	/**
	 * 获取普通api处理后的header参数map
	 *
	 * @param key
	 * @param params
	 * @param secret
	 * @return
	 */
	public static Map<String, String> getHeaderSignParamMap(Map<String, String> params, String key, String secret) {
		String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		params.put("key", key);
		params.put("ts", ts);
		String sign = getApiSign(params, secret);

		Map<String, String> header = new HashMap<String, String>();
		header.put("key", key);
		header.put("ts", ts);
		header.put("sign", sign);
		return header;
	}

	public static void main(String[] args) {
		System.out.println(getHeaderSignParamMap(new HashMap<String, String>(),"test","test"));
	}
}