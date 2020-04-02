package cis552project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CIS552ProjectUtils {

	public static String[] combineArrays(String[] a, String[] b) {
		int length = a.length + b.length;
		String[] result = new String[length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	public static List<String[]> readTable(String filePath, String tableName) throws IOException {
		String wholeFilePath = filePath + tableName;
		List<String[]> resultRows = new ArrayList<>();
		try (FileReader file = new FileReader(wholeFilePath + ".dat")) {
			BufferedReader fileStream = new BufferedReader(file);
			String temp = fileStream.readLine();
			while (temp != null) {
				resultRows.add(temp.split("\\|"));
				temp = fileStream.readLine();
			}
		} catch (FileNotFoundException ex) {
			try (FileReader file = new FileReader(wholeFilePath + ".csv")) {
				BufferedReader fileStream = new BufferedReader(file);
				String temp = fileStream.readLine();
				while (temp != null) {
					resultRows.add(temp.split("\\|"));
					temp = fileStream.readLine();
				}
			} catch (FileNotFoundException ex1) {
				Logger.getLogger(CIS552ProjectUtils.class.getName()).log(Level.SEVERE, null, ex1);
			}
		}
		return resultRows;

	}

	public static List<String> readCommands(String filePath) throws IOException {
		List<String> commandsList = new ArrayList<>();
		try (FileReader file = new FileReader(filePath)) {
			BufferedReader fileStream = new BufferedReader(file);
			String temp = fileStream.readLine();
			String previousString = temp;
			while (temp != null) {
				if (!temp.endsWith(";")) {
					previousString += " " + temp;
				} else {
					temp = previousString + " " + temp;
					previousString = "";
					commandsList.add(temp);
				}
				temp = fileStream.readLine();
			}
		}

		return commandsList;
	}

}
