package cis552project;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CIS552ProjectUtils {

	public static String[] combineArrays(String[] a, String[] b) {
		int length = a.length + b.length;
		String[] result = new String[length];
		System.arraycopy(a, 0, result, 0, a.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}

	public static List<String[]> readTable(String filePath) throws IOException {
		// File myObj = new File(filePath);
		FileReader file = new FileReader(filePath);
		BufferedReader fileStream = new BufferedReader(file);
		List<String[]> resultRows = new ArrayList<>();
		String temp = fileStream.readLine();
		while (temp != null) {
			resultRows.add(temp.split("\\|"));
			temp = fileStream.readLine();
		}
		return resultRows;
	}
}
