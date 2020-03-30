package io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

import cis552project.CIS552ProjectUtils;
import cis552project.Tuple;

public class TableScan {
	String dataPath = null;
	String tableName = null;
	RandomAccessFile raFile = null;
	Tuple nextTuple = null;

	public TableScan(String dataPath, String tableName) throws IOException {
		this.dataPath = dataPath;
		this.tableName = tableName;
		readTable();
	}

	private void readTable() throws IOException {
		try {
			raFile = new RandomAccessFile(dataPath + "\\" + tableName + ".dat", "r");
		} catch (FileNotFoundException ex1) {
			Logger.getLogger(CIS552ProjectUtils.class.getName()).log(Level.SEVERE, null, ex1);
		}
	}
	
	public Tuple getNextTuple() {
        	return nextTuple;
	}
	
	public boolean getHasNextTuple() throws IOException {
		String nextLine = raFile.readLine();
		nextTuple = new Tuple(nextLine);
        return nextTuple==null?false:true;
	}
	
	public void reset() throws IOException{
		raFile.seek(0);;
	}
}
