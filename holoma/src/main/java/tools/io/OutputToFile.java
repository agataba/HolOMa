package tools.io;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Ausgabe von Strings in Datei.
 * @author Max
 *
 */
public class OutputToFile {
	
	private int buffSize;
	private String pathFile;
	private ArrayList<String> buffer;
	
	
	public OutputToFile (int buffSize, String pathFile) {
		this.buffSize = (buffSize > 0) ? buffSize : 10;
		this.pathFile = pathFile;
		File file = new File(pathFile);
		if (file.exists()) file.delete();
		buffer = new ArrayList<String>();
	}

	public String getPath () { return this.pathFile; }
	
	public boolean isEmpty () { return buffer.isEmpty(); }
	
	public void changePath (String pathFileNew) {
		this.pathFile = pathFileNew;
	}
	
	public void close () {
		clearBuff();
	}
	
	public void addToBuff (String datum) {
		buffer.add(datum);
		if (buffer.size() > buffSize) {
			clearBuff();
		}
	}
	
	public void addToBuff(List<String> data) {
		if (data.size()<=buffSize-buffer.size()) {
			buffer.addAll(data);
		}
		else {
			for(String datum: data) {
				addToBuff(datum);
			}
		}
	}
	
	private void clearBuff() {
		PrintWriter writer = null;
		try {
			writer = new PrintWriter (new BufferedWriter (new FileWriter(pathFile, true)));
			for (String datum: buffer)
				writer.println(datum);
			writer.close();
			
			buffer.clear();
		} catch (IOException e) {
			System.err.println("Wrong path '"+pathFile+"'.");
			System.exit(1);
		}
	}

}

