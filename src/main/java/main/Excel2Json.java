package main;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by zhouqi on 2017/9/29.
 */
public class Excel2Json {
	public static void main(String[] args) throws Exception {
		InputStream instream = new FileInputStream("file/页面组件@0925.xlsx");
		Workbook excel = new XSSFWorkbook(instream);
		Sheet readsheet = excel.getSheetAt(2);
		JSONObject json = new JSONObject(new LinkedHashMap<String, Object>());
		json.put("全部", new Integer[]{-1});
		for (int rowNum = 1; rowNum <= readsheet.getLastRowNum(); rowNum++) {
			Row row = readsheet.getRow(rowNum);
			Cell cell1 = row.getCell(0);
			cell1.setCellType(Cell.CELL_TYPE_STRING);
			Cell cell2 = row.getCell(1);
			cell1.setCellType(Cell.CELL_TYPE_STRING);
			String val1 = cell1.getStringCellValue();
			String val2 = cell2.getStringCellValue();
			if (json.getJSONArray(val2) == null) {
				json.put(val2, new JSONArray());
			}
			json.getJSONArray(val2).add(Integer.valueOf(val1));
		}
		System.out.println(json.toJSONString());
		System.out.println(json.toJSONString().length());
	}
}
