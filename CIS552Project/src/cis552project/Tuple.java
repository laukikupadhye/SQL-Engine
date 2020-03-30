package cis552project;

public class Tuple {

	String[] lineElements = null;

	public Tuple(String tableLine) {
		this.lineElements = tableLine.split("\\|");
	}
	
	public String[] getObjectAsArray() {
		return lineElements;
	}

}
