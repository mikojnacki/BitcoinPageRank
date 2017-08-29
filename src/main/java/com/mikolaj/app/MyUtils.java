package com.mikolaj.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;

/**
 * Utilities
 */
public class MyUtils {

    public static void generateReport(String programName, String execDate, long totalTime) {
        try{
            PrintWriter writer = new PrintWriter(programName + "_" + execDate + ".txt", "UTF-8");
            writer.println("Program name: " + programName);
            writer.println("Execution date: " + execDate);
            writer.println("Execution time: " + String.valueOf(totalTime / 1000.0) + " seconds");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getCurrentDateTime() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return currentDateTime.toString();
    }
}
