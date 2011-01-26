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
    private int articleCount = 0;

    private long startTime = 0;
    private long stopTime = 0;
    private ArrayList<Number> timeStack;
    private ArrayList<String> checkedArticle;

    public WikipediaBenchmark() {
        try {
            Class.forName("org.gjt.mm.mysql.Driver").newInstance();
        } catch (Exception e) {
            System.out.println("Fehler beim laden der MySQL Treiber:"+e);
        }
        //initialisiere array
        this.checkedArticle = new ArrayList<String>();
    }

    public boolean limitReached() {
        return (this.articleCount>=this.limit);
    }

    private int findArticleInSQL(String title, int i, Connection conn) throws SQLException {
        if (this.limitReached()) return -1;
        i++;
        //this.addLog(i+": bei limit"+this.limit);
        Statement stmt = (Statement) conn.createStatement();
        String sqlCommand = "SELECT * FROM articles "+
                        "WHERE Title = \""+title+"\" LIMIT 1;\n";
        this.logSQL(sqlCommand);
        ResultSet articles,texts,links;
        articles = stmt.executeQuery(sqlCommand);
        if (articles.next()) {
            this.addLog("Artikel '"+title+"' ist vorhanden");
            String articleID = articles.getString("ID");
            this.articleCount++;
            //Inhalt suchen
            sqlCommand = "SELECT * FROM textindex WHERE ArticleID = "+articleID+" LIMIT 1000;";
            this.addLog("Alle Textabschnitte zu Artikel '"+title+"' raussuchen");
            this.logSQL(sqlCommand);
            texts = stmt.executeQuery(sqlCommand);
            int textsCount=0;
            while (texts.next()) {
                textsCount++;
            }
            this.addLog("Ins. "+textsCount+" Abschnitte gefunden");
            //Links suchen
            sqlCommand = "SELECT * FROM textindex_link WHERE ArticleID = "+articleID+" LIMIT 1000;";
            this.addLog("Alle Links zu Artikel '"+title+"' raussuchen");
            this.logSQL(sqlCommand);
            links = stmt.executeQuery(sqlCommand);
            int linksCount=0;
            String nextArticleTitle;
            int found;
            while (links.next()) {
                if (this.checkedArticle.contains(links.getString("Link"))) {
                    //Artikel wurde bereits aufgerufen
                } else {
                    //Artikel wurde noch nicht aufgerufen, also weiter
                    nextArticleTitle = links.getString("Link");
                    this.checkedArticle.add(nextArticleTitle);
                    //abbruch, falls limit erreicht ist
                    if (i>this.limit) return -1;
                    else {
                        return this.findArticleInSQL(nextArticleTitle, i, conn);
                        //if (found>0) return found;
                    }
                }
                linksCount++;
            }
            this.addLog("Ins. "+linksCount+" Verlinkungen gefunden gefunden");
            //this.addLog(this.checkedArticle.toString());
        } else {
            return 0;
        }
        //String articleID = "";
        return -1;
    }

    public void searchArticle(String text, int limit) {
        this.startTimer();
        this.limit=limit;
        try {
            if (this.isSQL()) {
                this.addLog("Öffne MySQL Verbindung");
                Connection conn = this.openConnection();
                //Artikel suchen
                this.articleCount = 0;
                this.resetTimer();
                while (!this.limitReached()) {
                    this.findArticleInSQL(text, 0, conn);
                }
                this.logTimer();
                this.addLog("Es wurden insg. "+this.articleCount+" Artikel rausgesucht");

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
        //this.startTime=System.nanoTime();
        this.startTime=System.currentTimeMillis();
    }

    public void resetTimer() {
        this.startTimer();
    }

    public long measureTimer() {
        this.stopTime=System.currentTimeMillis();
        long difference = this.stopTime-this.startTime;
        //this.timeStack.add(difference);
        difference=Math.round(difference);
        return difference;
    }

    public void logTimer() {
        double time = this.measureTimer();
        time = Math.round(time);
        time = time/1000;
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
