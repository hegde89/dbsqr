package edu.unika.aifb.dbsqr.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import org.apache.log4j.Logger;

public class Log {
	
	private static final Logger log = Logger.getLogger(Log.class);
	
	private String fileName;
	private BufferedWriter outFile;
	
	public Log(String fileName){
		this.fileName = fileName;
	}
	
	public void open(){
		try{
			this.outFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.fileName)));
		}
		catch(Exception e){
			log.warn("Error while creating log file: " + this.fileName);
			log.warn(e.getClass() + ":" + e.getMessage());
		}
	}
	
	public void openAppend(){
		try{
			this.outFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.fileName, true)));
		}
		catch(Exception e){
			log.warn("Error while creating log file: " + this.fileName);
			log.warn(e.getMessage());
		}
	}

	public void writeLog(String message){
		try{
			this.outFile.write(message);
			this.outFile.newLine();
			this.outFile.flush();
		}
		catch(Exception e){
			log.warn("Error while writting data to the log file");
			log.warn(e.getClass() + ":" + e.getMessage());
		}
	}
	
	public void close(){
		try{
			this.outFile.close();
		}
		catch(Exception e){
			log.warn("Error while closing the log file");
			log.warn(e.getClass() + ":" + e.getMessage());
		}
	}
}
