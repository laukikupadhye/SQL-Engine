package cis552project.iterator;

import java.util.Arrays;

public class Tuple {

	public String[] resultRow = null;

	public Tuple(String[] resultRow) {
		this.resultRow = resultRow;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(resultRow);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (!Arrays.equals(resultRow, other.resultRow))
			return false;
		return true;
	}

}
