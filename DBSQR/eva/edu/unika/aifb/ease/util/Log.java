package edu.unika.aifb.ease.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class Log {
	
	private String fileName;
	private BufferedWriter outFile;
	
	public Log(String fileName){
		this.fileName = fileName;
	}
	
	public void Open(){
		try{
			this.outFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.fileName)));
		}
		catch(Exception e){
			System.out.println("Error while creating log file: " + this.fileName);
			System.out.println(e.getClass() + ":" + e.getMessage());
		}
	}
	
	public void OpenAppend(){
		try{
			this.outFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.fileName, true)));
		}
		catch(Exception e){
			System.out.println("Error while creating log file: " + this.fileName);
			System.out.println(e.getClass() + ":" + e.getMessage());
		}
	}

	public void WriteLog(String message){
		try{
			System.out.println(message);
			this.outFile.write(message);
			this.outFile.newLine();
			this.outFile.flush();
		}
		catch(Exception e){
			System.out.println("Error while writting data to the log file");
			System.out.println(e.getClass() + ":" + e.getMessage());
		}
	}
	
	public void Close(){
		try{
			this.outFile.close();
		}
		catch(Exception e){
			System.out.println("Error while closing the log file");
			System.out.println(e.getClass() + ":" + e.getMessage());
		}
	}
}
