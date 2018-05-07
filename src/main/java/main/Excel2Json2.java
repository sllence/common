package main;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedHashMap;

/**
 * Created by zhouqi on 2017/9/29.
 */
public class Excel2Json2 {
	public static void main(String[] args) throws Exception {
		String filePath = "file/经纪人等级对应.xls";
		InputStream instream = new FileInputStream(filePath);
		Workbook excel;
		if (filePath.endsWith(".xls")) {
			excel = new HSSFWorkbook(instream);
		} else {
			excel = new XSSFWorkbook(instream);
		}
		Sheet readsheet = excel.getSheetAt(0);
		JSONArray jsonArray = new JSONArray();
		Row HeadRow = readsheet.getRow(0);
		for (int rowNum = 1; rowNum <= readsheet.getLastRowNum(); rowNum++) {
			JSONObject json = new JSONObject();
			Row row = readsheet.getRow(rowNum);
			for (Cell headCell : HeadRow) {
				Cell cell = row.getCell(headCell.getColumnIndex());
				if (cell==null){
					json.put(headCell.getStringCellValue(), "");
				}else {
					cell.setCellType(Cell.CELL_TYPE_STRING);
					json.put(headCell.getStringCellValue(), cell.getStringCellValue());
				}
			}
			jsonArray.add(json);
		}
		System.out.println(jsonArray);
	}
}
