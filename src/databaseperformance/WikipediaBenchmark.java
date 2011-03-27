package databaseperformance;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

//http://www.mongodb.org/display/DOCS/Java+Types
import java.util.regex.Pattern;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

/**
 *
 * @author philipp staender
 */
public class WikipediaBenchmark {

    public String type = "mysql";
    public String database = "";
    public String collection = "";
    public String username = "root";
    public String password = "root";
    public String host = "localhost";
    public int port = 3306;
    public boolean searchInSubtitles = false;
    public boolean searchInContent = false;
    public boolean useRegularExpressions = false;
    public boolean logSQL = false;
    public boolean logVerbose = false;
    public boolean logNoSQL = false;
    public boolean log = true;

    public int limit = 1000;
    private int articleCount = 0;

    private long startTime = 0;
    private long stopTime = 0;
    private ArrayList<Number> timeStack;
    private ArrayList<String> checkedArticle;
    private ArrayList<String> finalLogs;

    public WikipediaBenchmark() {
        try {
            Class.forName("org.gjt.mm.mysql.Driver").newInstance();
        } catch (Exception e) {
            System.out.println("Fehler beim laden der MySQL Treiber: "+e.getMessage());
        }
        //initialisiere array
        this.checkedArticle = new ArrayList<String>();
        this.finalLogs = new ArrayList<String>();
    }

    public boolean limitReached() {
        return (this.articleCount>=this.limit);
    }

    private int findArticleInNoSQL(String title, int i, DB db) {
        if (this.limitReached()) return -1;
        i++;
        DBCollection articles = db.getCollection("articles");
        String searchField = "title";
        if (this.searchInSubtitles) searchField = "sections.subtitle";
        if (this.searchInContent) searchField = "section.content";
        BasicDBObject query = new BasicDBObject(
                searchField,
                ((this.useRegularExpressions) ? Pattern.compile(title, Pattern.CASE_INSENSITIVE) : title) 
                );

        
        if (this.useRegularExpressions) this.logNoSQL("db.articles.find({\""+searchField+"\":/"+title+"/});");
        else this.logNoSQL("db.articles.findOne({\""+searchField+"\":'"+title+"'});");
        
        DBObject article = articles.findOne(query);
        
        if (article != null) this.articleCount++;
        else return -1;
        
        String nextArticleTitle = "";
        
        List<Object> links = (List<Object>) article.get("links");
        this.addOptionalLog("Insg. "+links.size()+" Verlinkungen im Artikel '"+title+"' gefunden");
        for (Object link : links) {
            if (this.checkedArticle.contains((String)link)) {
                //Artikel wurde bereits aufgerufen
            } else {
                //Artikel wurde noch nicht aufgerufen, also weiter
                nextArticleTitle = link.toString();
                this.checkedArticle.add(nextArticleTitle);
                //abbruch, falls limit erreicht ist
                if (i>this.limit) return -1;
                else {
                    return this.findArticleInNoSQL(nextArticleTitle, i, db);
                }
            }
        }
        return -1;
    }

    private int findArticleInSQL(String title, int i, Connection conn) throws SQLException {
        if (this.limitReached()) return -1;
        i++;
        Statement stmt = (Statement) conn.createStatement();
        String whereCondition = "Title";
        String sqlTitle = title;
        if (this.searchInSubtitles) {
            whereCondition = "Content";
            sqlTitle = "== "+title+" ==";
        }
        if (this.searchInContent) {
            whereCondition = "Content";
        }
        if ((this.useRegularExpressions) || (this.searchInSubtitles)) whereCondition += " LIKE \"%**title**%\"";
        else whereCondition += " = \"**title**\"";
        whereCondition = whereCondition.replace("**title**",WikipediaBenchmark.sqlEscape(sqlTitle));
        String sqlCommand = "SELECT * FROM articles "+
                        "WHERE "+whereCondition+" LIMIT 1;\n";
        this.logSQL(sqlCommand);
        ResultSet articles,texts;
        articles = stmt.executeQuery(sqlCommand);
        if (articles.next()) {
            String articleID = articles.getString("ID");
            this.articleCount++;
            //Links suchen
            this.logSQL(sqlCommand);
            String[] links = articles.getString("Links").split(",");
            this.addOptionalLog("Insg. "+links.length+" Verlinkungen im Artikel '"+title+"' gefunden");
            
            int linksCount=0;
            String nextArticleTitle;
            int found;
            for (String link : links) {

                if (this.checkedArticle.contains(link)) {
                    //Artikel wurde bereits aufgerufen
                } else {
                    //Artikel wurde noch nicht aufgerufen, also weiter
                    nextArticleTitle = link;
                    this.checkedArticle.add(nextArticleTitle);
                    //abbruch, falls limit erreicht ist
                    if (i>this.limit) return -1;
                    else {
                        return this.findArticleInSQL(nextArticleTitle, i, conn);
                    }
                }
                linksCount++;
            }
        } else {
            return 0;
        }
        //String articleID = "";
        return -1;
    }

    public void searchArticleInMySQL(String text, int limit) {
        this.startTimer();
        this.limit=limit;
        try {
            //mysql
            this.type="mysql";
            this.addLog("Oeffne MySQL Verbindung");
            Connection conn = this.openConnection();
            this.checkedArticle = new ArrayList<String>();
            this.articleCount = 0;
            //Artikel suchen
            this.articleCount = 0;
            this.addLog("Start mysql Bechnmark...");
            this.resetTimer();
            while (!this.limitReached()) {
                this.findArticleInSQL(text, this.articleCount, conn);
            }
            this.addFinalLog(this.logTimer("MySQL"));
            this.addLog(this.addLog("Es wurden insg. "+this.articleCount+" Artikel via MySQL rausgesucht"));
        } catch (Exception e) {
            System.err.println("Fehler bei der Abfrage der MySQL-DB: "+e.getMessage());
            System.exit(-1);
        }
    }
    
    public void searchArticleInMongoDB(String text, int limit) {
        this.startTimer();
        this.limit=limit;
        try {
            //nosql
            this.type="mongodb";
            this.addLog("Oeffne mongodb Verbindung");
            Mongo m = new Mongo();
            DB db = m.getDB( "wikipedia" );
            this.checkedArticle = new ArrayList<String>();
            this.articleCount = 0;
            this.addLog("Start mongodb Bechnmark...");
            this.resetTimer();
            while (!this.limitReached()) {
                this.findArticleInNoSQL(text, this.articleCount, db);
            }
            this.addFinalLog(this.logTimer("MongoDB"));
            this.addLog(this.addLog("Es wurden insg. "+this.articleCount+" Artikel via mongodb rausgesucht"));
            System.out.println("========");
            System.out.println(this.getFinalLogMessage());
            System.out.println("========");
            
        } catch (Exception e) {
            System.err.println("Fehler bei der Abfrage der MongoDB: "+e.getMessage());
            System.exit(-1);
        }
    }

    private String addLog(String message) {
        if (this.log) System.out.println(message);
        return message;
    }
    
    private String addOptionalLog(String message) {
        if (this.logVerbose) System.out.println(message);
        return message;
    }

    public String addFinalLog(String message) {
        this.finalLogs.add(message);
        return message;
    }

    public String getFinalLogMessage() {
        String message = "";
        for (Object line : this.finalLogs) {
            message += line+"\n";
        }
        return message.trim();
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

    public String logTimer() {
        return logTimer("");
    }

    public String logTimer(String message) {
        double time = this.measureTimer();
        time = Math.round(time);
        time = time/1000;
        message = time+" [s] \t"+message;
        this.addLog(message);
        return message;
    }

    public void logSQL(String sql) {
        if (this.logSQL) this.addLog("mysql> "+sql);
    }

    public void logNoSQL(String command) {
        if (this.logNoSQL) this.addLog("mongodb> "+command);
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
    
    public static String sqlEscape(String str) {
		if (str.length() == 0)
			return ""; //TODO "NULL",too ?
		final int len = str.length();
		StringBuffer sql = new StringBuffer(len * 2);
		synchronized (sql) { //only for StringBuffer
		//sql.append('\'');
		for (int i = 0; i < len; i++) {
			char c = str.charAt(i);
			switch (c) {
			case '\u0000':
				sql.append('\\').append('0');
				break;
			case '\n':
				sql.append('\\').append('n');
				break;
			case '\r':
				sql.append('\\').append('r');
				break;
			case '\u001a':
				sql.append('\\').append('Z');
				break;
			case '"':
			case '\'':
			case '\\':
				sql.append('\\');
				// fall through
			default:
				sql.append(c);
				break;
			}
		}
		//sql.append('\'');
		return sql.toString();
		}
	}

    
}
