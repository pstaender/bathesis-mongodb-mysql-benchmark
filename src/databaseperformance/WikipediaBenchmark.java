/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package databaseperformance;

//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.sql.*;
//import com.mysql.jdbc.*;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @author philipp
 */
public class WikipediaBenchmark {

    public String type = "mysql";
    public String database = "";
    public String collection = "";
    public String username = "root";
    public String password = "root";
    public String host = "localhost";
    public int port = 3306;

    public int limit = 1000;

    private long startTime = 0;
    private long stopTime = 0;
    private ArrayList<Number> timeStack;

    public WikipediaBenchmark() {
        try {
            Class.forName("org.gjt.mm.mysql.Driver").newInstance();
        } catch (Exception e) {
            System.out.println("Fehler beim laden der MySQL Treiber:"+e);
        }
    }

    public void searchArticle(String text, int limit) {
        this.startTimer();
        try {
            if (this.isSQL()) {
                this.addLog("Öffne MySQL Verbindung");
                Connection conn = this.openConnection();
                Statement stmt = (Statement) conn.createStatement();
                //Artikel suchen
                this.resetTimer();
                String sqlCommand = "SELECT * FROM articles "+
                        "WHERE Title = \""+text+"\" LIMIT 1;\n";
                this.logSQL(sqlCommand);
                this.resetTimer();
                ResultSet resultSet;
                resultSet = stmt.executeQuery(sqlCommand);
                this.logTimer();

                String articleID = "";

                while (resultSet.next()) {
                    this.addLog("Artikel '"+resultSet.getString("Title")+"' gefunden");
                    articleID = resultSet.getString("ID");
                }

                this.addLog("Suche Inhalt zu ArtikelID ='"+articleID+"'");
                //Inhalt suchen
                sqlCommand = "SELECT * FROM textindex WHERE ArticleID = "+articleID+" LIMIT 100;";
                this.logSQL(sqlCommand);
                this.resetTimer();
                resultSet = stmt.executeQuery(sqlCommand);
                this.logTimer();

                String textIndexID="";
                while (resultSet.next()) {
                    this.addLog("Alle Links zu Artikel '"+text+"' finden");
                    articleID = resultSet.getString("ID");
                }

            } else {
                //nosql
            }
        } catch (Exception e) {
            System.out.println("Fehler bei der Abfrage der Datenbank: "+e.getMessage());
        }
    }

    private String addLog(String message) {
        System.out.println(">"+message+"...");
        return message;
    }

    public Boolean isSQL() {
        return (this.type.toLowerCase()=="mysql") ? true : false;
    }

    public Boolean isNoSQL() {
        return !(this.isSQL());
    }

    public void startTimer() {
        this.startTime=System.nanoTime();
    }

    public void resetTimer() {
        this.startTimer();
    }

    public long measureTimer() {
        this.stopTime=System.nanoTime();
        long difference = this.stopTime-this.startTime;
        //this.timeStack.add(difference);
        difference=Math.round(difference);
        return difference;
    }

    public void logTimer() {
        double time = this.measureTimer();
        time = time/10000000;
        this.addLog("Gemessene Zeit: "+time+" [s]");
    }

    public void logSQL(String sql) {
        this.addLog("[SQL]"+sql);
    }

    public long countTimer() {
        long sum = 0;
        for (int i=0; i<this.timeStack.size();i++) sum = sum+this.timeStack.get(i).longValue();
        return sum;
    }

    private Connection openConnection() {
        if (this.isSQL()) {
            String dbURL = "jdbc:mysql://"+this.host+":"+this.port+"/"+this.database;
            //try {
            //    Class.forName("com.mysql.jdbc.Driver").newInstance();
            //} catch(Exception e) {
            //    System.out.println("Fehler beim Laden der MySQL Klasse:"+e.getMessage());
            //}
            try {
                Connection conn = (Connection) DriverManager.getConnection(dbURL,this.username,this.password);
                return conn;
                } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
                }
        }
        return null;
    }

    
}
