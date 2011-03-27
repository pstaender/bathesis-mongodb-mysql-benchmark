package databaseperformance;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.ListIterator;

/**
 *
 * @author Philipp Ständer
 */
public class Main {


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        //ermögliche das empfangen von Texteingabe
        //StreamTokenizer Input=new StreamTokenizer(System.in);
        List<String> arguments = Arrays.asList(args);
        if (!(arguments.contains("--silent"))) {
            System.out.println("****** Wikipedia MySQL/MongoDB Benchmarktest *****");
            System.out.println("(c) 2010-2011 philipp staender");
            System.out.println("-h            Hilfe (fuer alle Parameter auflisten)\n");
        }
        if ( (arguments.contains("-h")) ) {
            System.out.println("Startparameter:\n===============");
            System.out.println("-v            Ausfuehrliches Logging, Performance beachten\n");
            System.out.println("--mongodb     Durchsuche die MongoDB");
            System.out.println("--mysql       Durchsuche die MySQL DB");
            System.out.println("--regex       Durchsuche mit reg. Ausdruecken / LIKE %% Statemnts");
            System.out.println("--exact       Durchsuche nach exakten Treffern");
            System.out.println("-l100         Limit setzen, hier z.B. 100");
            System.out.println("-tCoca_Cola   Nach Ueberschrift suchen, hier z.B. Coca Cola");
            System.out.println("-sGeschichte  Nach Unterueberschrift suchen, hier z.B. Geschichte");
            System.out.println("-cTo_be_or    Inhalt durchsuchen");
            System.out.println("--silent      Keine Erklaerungen");
//            System.out.println("Bei Arbeitsspeicherproblemen kann folgender Ausfuehrungsparameter helfen:");
//            System.out.println("java -Xms1024m -Xmx2048m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:PermSize=128m -XX:MaxPermSize=128m -jar ...");
            System.out.println("");
            System.exit(0);
        }
        try {
            
            ListIterator<String> li = arguments.listIterator();
            
            int limit = 0;
            String databaseType = "";
            String useRegularExpression = "";
            String searchInSubtitles = "";
            String searchtext = "";
            boolean searchInContent = false;
            
            BufferedReader lineOfText = new BufferedReader(new InputStreamReader(System.in));
            
            while(li.hasNext()) {
                int i = li.nextIndex();
                String next = li.next();
                if(Pattern.matches("^-l.+", next)) {
                  limit = Integer.parseInt(next.substring(2));
                }
                if(Pattern.matches("^-t.+", next)) {
                  searchtext = next.substring(2).replace("_", " ");
                  searchInSubtitles = "h";
                }
                if(Pattern.matches("^-s.+", next)) {
                  searchtext = next.substring(2).replace("_", " ");
                  searchInSubtitles = "u";
                }
                if(Pattern.matches("^-c.+", next)) {
                  searchtext = next.substring(2).replace("_", " ");
                  searchInContent = true;
                }
                if(Pattern.matches("^--mongodb$", next)) {
                  databaseType = (databaseType.equals("s")) ? "a" : "n";
                }
                if(Pattern.matches("^--mysql$", next)) {
                  databaseType = (databaseType.equals("n")) ? "a" : "s";
                }
                if(Pattern.matches("^--regex$", next)) {
                  useRegularExpression = "j";
                }
                if(Pattern.matches("^--exact$", next)) {
                    useRegularExpression = "n";
                }
            }
 
            while (!(searchtext.trim().length()>0)) {
                System.out.print("Artikel (z.B. Alfred Hitchcock): ");
                lineOfText = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
                searchtext = lineOfText.readLine();
                if (searchtext.trim().length()==0) searchtext = "Alfred Hitchcock";
                else System.out.println("Gewaehlter Artikel: '"+searchtext+"'");
            }

            while (!(limit>=1)) {
                System.out.print("Max. Abfragen [100]: ");
                lineOfText = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
                String readedLine = lineOfText.readLine().trim();
                if (readedLine.equals("")) limit = 100;
                else limit = Integer.parseInt(readedLine);
                System.out.println("Gewaehltes Limit: "+limit+"");
            }
            
            while (!((databaseType.equals("s")) || (databaseType.equals("n")) || (databaseType.equals("a")) )) {
                System.out.print("Welche Datenbanktypen [s]ql, [n]osql, [a]lle: [a] ");
                lineOfText = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
                databaseType = lineOfText.readLine();
                if (databaseType.trim().equals("")) databaseType="a";
                System.out.println("Gewaehlte Option: ["+databaseType+"]");
            }

            while (!((useRegularExpression.equals("j")) || (useRegularExpression.equals("n")))) {
                System.out.print("Suche mit regulaeren Ausdruecken / `foo LIKE '%bar%'`-Statements [j]/[n] : [n] ");
                lineOfText = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
                useRegularExpression = lineOfText.readLine();
                if ((useRegularExpression.trim().equals(""))) useRegularExpression="n";
                System.out.println("Gewaehlte Option: ["+useRegularExpression+"]");
            }
            
            while (!((searchInSubtitles.equals("h")) || (searchInSubtitles.equals("u")) || (searchInContent))) {
                System.out.print("Duchsuche [h]aupt-,[u]nterueberschriften oder [i]nhalt: [h] ");
                lineOfText = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
                searchInSubtitles = lineOfText.readLine();
                if ((searchInSubtitles.trim().equals(""))) searchInSubtitles="h";
                if (searchInSubtitles.equals("i")) searchInContent=true;
                System.out.println("Gewaehlte Option: ["+searchInSubtitles+"]");
            }
            
            WikipediaBenchmark bench = new WikipediaBenchmark();
            bench.startTimer();
            bench.type="mysql";
            bench.username="root";
            bench.password="root";
            bench.database="nosql";
            bench.logNoSQL=(arguments.contains("-v"));
            bench.logSQL=(arguments.contains("-v"));
            bench.logVerbose=(arguments.contains("-v"));
            bench.useRegularExpressions=(useRegularExpression.equals("j"));
            bench.searchInSubtitles=(searchInSubtitles.equals("u"));
            bench.searchInContent=searchInContent;
            if (bench.searchInContent) bench.useRegularExpressions=true;
            
            if (!(arguments.contains("--silent"))) {
                System.out.println("Suchbegriff: " + searchtext);
                System.out.println("Maximal Abfragen: " + limit);
                bench.log = true;
            } else {
                bench.log = false;
            }

            if ((databaseType.equals("s")) || (databaseType.equals("a"))) bench.searchArticleInMySQL(searchtext,limit);
            if ((databaseType.equals("n")) || (databaseType.equals("a"))) bench.searchArticleInMongoDB(searchtext,limit);

            if (!(arguments.contains("--silent"))) {
                System.out.println("fertig :)");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
