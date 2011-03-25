package databaseperformance;

import java.io.*;

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
        try {
            System.out.print("Suchbegriff eingeben (z.B. Alfred Hitchcock): ");
            BufferedReader lineOfText = new BufferedReader(new InputStreamReader(System.in));
            String searchtext = lineOfText.readLine();

            while (!(searchtext.trim().length()>0)) searchtext = "Alfred Hitchcock";

            int limit = 0;

            while (!(limit>=1)) {
                System.out.print("Max. Abfragen (z.B. 1000): ");
                lineOfText = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
                limit = Integer.parseInt(lineOfText.readLine());
            }
            

            System.out.println("Suchbegriff: " + searchtext);
            System.out.println("Maximal Abfragen: " + limit);


            WikipediaBenchmark bench = new WikipediaBenchmark();
            bench.startTimer();
            bench.type="mysql";
            bench.username="root";
            bench.password="root";
            bench.database="nosql";
            bench.logNoSQL=true;
            bench.logSQL=true;
            bench.useRegularExpressions=true;
            bench.searchInSubtitles=false;
            
//            bench.searchArticleInMySQL(searchtext,limit);
            bench.searchArticleInMongoDB(searchtext,limit);

            System.out.println("fertig");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
