package tools.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;

public class InputFromFile {
	
	/** The input file. */
	private final File FILE;
	/** A regular expression which should be true in every input line. */
	private final String REGULAR_EXPR;
	/** The delimiter of each column per row. */
	private final String COLUMN_DEL; 
	
	/** Number of columns. */
	private int noCol = -1;
	/** The content of the input file. */
	private List<String> fileContent = new ArrayList<String>();
	/** The content of the input file, separated by columns. */
	private List<String[]> fileContentAsCol = new ArrayList<String[]>();
	/** The content of the input file as a column store. */
	private List<List<String>> fileColumns = new ArrayList<List<String>>();
	
	
	
	public static class Builder {
		// required parameters
		private final File FILE;		
		//optional parameters - initialised to default values
		private String regularExpr = null;
		private String columnDel = "\t"; 
		
		public Builder (String fileLocation) {
			if (fileLocation == null || fileLocation.equals(""))
				throw new IllegalArgumentException("No valid (empty) file location.");
			this.FILE = new File(fileLocation);
			if (!this.FILE.exists()) 
				throw new IllegalArgumentException("Input file "+fileLocation+" does not exist.");
		}
		
		/**
		 * Specifies the regular expression which holds for each line. Default: none.
		 * @param val The regular expression.
		 * @return Builder object.
		 */
		public Builder regularExpr (String val) {
			this.regularExpr = val;
			return this;
		}
		
		/**
		 * Specifies the delimiter of the columns. Default: '\t'
		 * @param val The delimiter of the columns.
		 * @return Builder object.
		 */
		public Builder columnDel (String val) {
			this.columnDel = val;
			return this;
		}
		
		/**
		 * Builds the InputFromFile object.
		 * @return InputFromFile object.
		 */
		public InputFromFile build() {
			return new InputFromFile(this);
		}
		
		
	}
	/** 
	 * Constructor;
	 * Example: InputFromFile in = new InputFromFile.Builder(yourFileLocation).noCol(2);
	 * @param Builder Builder for an InputFromFile object.
	 */
	public InputFromFile (Builder builder) {
		this.FILE = builder.FILE;
		this.REGULAR_EXPR = builder.regularExpr;
		this.COLUMN_DEL = builder.columnDel;
	}
	
	/**
	 * The old Constructor.
	 * @param location
	 */
	public InputFromFile (String location) {
		Builder b = new Builder(location);
		this.FILE = b.FILE;
		this.REGULAR_EXPR = b.regularExpr;
		this.COLUMN_DEL = b.columnDel;
	}
	
	
	/**
	 * Return the content of file.
	 * @return The file content.
	 */
	public List<String> getFileContent () {
		if (fileContent.isEmpty())
			readFile();
		return fileContent;
	}
	
	/**
	 * Returns the content of the file whereby each line is split into its columns.
	 * @return The column separated file content.
	 */
	public List<String[]> getContentSplitByColumn () {
		if (this.fileContentAsCol.isEmpty())
			readFileLinesAsCol();
		return this.fileContentAsCol;
	}
	
	public List<List<String>> getContentAsColumns () {
		if (this.fileColumns.isEmpty())
			readColumns();
		return this.fileColumns;
	}
	
	/**
	 * Closes the input. All Lists are cleared.
	 * @return 'true' iff the content is cleared.
	 */
	public boolean close () {
		if (fileContent.size()==0 && fileContentAsCol.size()==0) return false;
		else {
			fileContent.clear();
			fileContentAsCol.clear();
			return true;
		}
	}
	
	private void readColumns() {
		for (String line : getFileContent()) {
			addToCols (line);
		}
	}
	
	/** 
	 * Adds values to the columns of this file content. 
	 * @param line The current line in the file.
	 */
	private void addToCols (String line) {
		String[] data = line.split(COLUMN_DEL);
		checkNoCols(data.length);
		if (this.fileColumns.isEmpty())
			for (int i=0; i<data.length; i++) {
				List<String> l = new ArrayList<String>();
				l.add(data[i]);
				this.fileColumns.add(l);
			}
		else
			for (int i=0; i<data.length; i++) {
				this.fileColumns.get(i).add(data[i]);
			}		
	}
	
	/**
	 * Checks that each line has the same the number of columns.
	 * @param noColInLine Count of columns for the current line.
	 */
	private void checkNoCols (int noColInLine) {
		// check whether number of columns is valid
		if (this.noCol == -1) this.noCol = noColInLine;
		else {
			if (this.noCol != noColInLine)
				throw new IllegalArgumentException("Wrong number of columns for delimiter "+this.COLUMN_DEL);
		}
	}
	
	/** Reads the content of file column separated and buffers it. */
	private void readFileLinesAsCol() {		
		for (String line : getFileContent()) {
			saveLineAsCol(line);
		}		
	}
	
	/**
	 * Saves a given line column separated.
	 * @param line A given line of the file.
	 */
	private void saveLineAsCol (String line) {
		String[] data = line.split(COLUMN_DEL);
		checkNoCols (data.length);
		// add data to content list
		this.fileContentAsCol.add(data);
	}
	
	
	/** Reads content of the file and buffers it. */
	private void readFile() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader (FILE));
			String line;
			while ((line = reader.readLine()) != null) {
				if (REGULAR_EXPR != null) {
					try {
						if (!checkLine(line))
							System.err.println("Line "+line+" does not match the regular expression '"+REGULAR_EXPR+"'!");
						else
							fileContent.add(line);
					} catch (PatternSyntaxException e) {
						System.err.println("Invalid regular expression: "+REGULAR_EXPR);
						System.exit(501);
					}
				}
				else
					fileContent.add(line);			
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	
	/**
	 * Checks the line against an regular expression.
	 * @param line The line.
	 * @return 'true' iff line matches regular expression.
	 * @throws PatternSyntaxException Invalid regular expression.
	 */
	private boolean checkLine (String line) throws PatternSyntaxException {
		return line.matches(REGULAR_EXPR);
	}
	

}
