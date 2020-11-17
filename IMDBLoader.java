
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;


/**
 * @author : Omkar Sarde
 * @version : 1.0
 */

/**
 * This program connects to database using jdbc driver and loads the database
 * from gzip files
 */
public class IMDBLoader {

    static Connection conn = null;
    static PreparedStatement ps = null;
    static Statement stmt = null;
    String folderToAssignment;
    //static String url;
//    static String dburl ;
//    static String user;
//    static String pwd;
//    static String pathToData;
    File name, title, ratings, principals;
    static HashMap<String, Integer> genre = new HashMap<>();

    //    public IMDBLoader(String folderToAssignment, String url, String user, String pwd, String pathToData){
//        this.folderToAssignment = folderToAssignment;
//        this.url = url;
//        this.user = user;
//        this.pwd = pwd;
//        this.pathToData = pathToData;
//
//    }
    public static void main(String[] args) throws Exception {
        try {

            String dbURL = args[0];
            String user = args[1];
            String pwd = args[2];
            String pathToIMDBData = args[3];

            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, user, pwd);
            stmt = conn.createStatement();
            System.out.println("Connected to the database");

            createTables();
            insertPersonTable(dbURL,user,pwd,pathToIMDBData);
            insertMovieTable(dbURL,user,pwd,pathToIMDBData);
            insertGenre(dbURL,user,pwd,pathToIMDBData);
            insertHasGenre(dbURL,user,pwd,pathToIMDBData);
            insertRatings(dbURL,user,pwd,pathToIMDBData);
            insertInProfessions(dbURL,user,pwd,pathToIMDBData);
            System.out.println("Completed All Insertions!!! ");

        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
            }
        }
        System.out.println("Disconnected from database");
    }

    /**
     * This method creates tables for
     * all entities from ER diagram
     * @throws SQLException
     */
    public static void createTables() throws SQLException {
        stmt = conn.createStatement();
        stmt.executeUpdate("USE imdb");
        // table 1
        String personTable = "CREATE TABLE IF NOT EXISTS Person" +
                "( personID INTEGER not null, " +
                " personName VARCHAR(200)," +
                " birthYear INTEGER(4) ," +
                " deathYear INTEGER(4) ," +
                " PRIMARY KEY(personID) )";
        ps = conn.prepareStatement(personTable);
        ps.executeUpdate(personTable);

        // table 2
        String movieTable = "CREATE TABLE IF NOT EXISTS Movie " +
                "( movieID INTEGER not null," +
                " title VARCHAR(1000) not null, " +
                " releaseYear INTEGER(4) null, " +
                " runtime INTEGER , " +
                " rating FLOAT , " +
                " numberOfVotes INTEGER , " +
                "PRIMARY KEY (movieID) )";
        ps = conn.prepareStatement(movieTable);
        ps.executeUpdate(movieTable);

        // table 3
        String genreTable = "CREATE TABLE IF NOT EXISTS GENRE " +
                "( genreID INTEGER not null, " +
                " genreName VARCHAR(500), " +
                " PRIMARY KEY (genreID) )";
        ps = conn.prepareStatement(genreTable);
        ps.executeUpdate(genreTable);

        // table 4
        String hasGenre = "CREATE TABLE IF NOT EXISTS HasGENRE " +
                "( genreID INTEGER not null, " +
                " movieID INTEGER not null, " +
                " PRIMARY KEY (genreID, movieID), " +
                " FOREIGN KEY (genreID) REFERENCES GENRE(genreID)," +
                " FOREIGN KEY (movieID) REFERENCES Movie(movieID) )";
        ps = conn.prepareStatement(hasGenre);
        ps.executeUpdate(hasGenre);


        // table 5
        String actedIn = "CREATE TABLE IF NOT EXISTS ActedIN " +
                "( personID INTEGER not null, " +
                " movieID INTEGER not null, " +
                " PRIMARY KEY (personID, movieID), " +
                " FOREIGN KEY (personID) REFERENCES Person(personID)," +
                " FOREIGN KEY (movieID) REFERENCES Movie(movieID)) ";
        ps = conn.prepareStatement(actedIn);
        ps.executeUpdate(actedIn);

        // table 6
        String composedBy = "CREATE TABLE IF NOT EXISTS ComposedBy " +
                "( personID INTEGER not null, " +
                " movieID INTEGER not null, " +
                " PRIMARY KEY (personID, movieID), " +
                " FOREIGN KEY (personID) REFERENCES Person(personID)," +
                " FOREIGN KEY (movieID) REFERENCES movie(movieID))";
        ps = conn.prepareStatement(composedBy);
        ps.executeUpdate(composedBy);

        // table 7
        String directedBy = "CREATE TABLE IF NOT EXISTS DirectedBy " +
                "( personID INTEGER not null, " +
                " movieID INTEGER not null, " +
                " PRIMARY KEY (personID, movieID), " +
                " FOREIGN KEY (personID) REFERENCES Person(personID)," +
                " FOREIGN KEY (movieID) REFERENCES movie(movieID))";
        ps = conn.prepareStatement(directedBy);
        ps.executeUpdate(directedBy);

        // table 8
        String editedBy = "CREATE TABLE IF NOT EXISTS EditedBy " +
                "( personID INTEGER not null, " +
                " movieID INTEGER not null, " +
                " PRIMARY KEY(personID, movieID), " +
                " FOREIGN KEY (personID) REFERENCES Person(personID)," +
                " FOREIGN KEY (movieID) REFERENCES movie(movieID) )";
        ps = conn.prepareStatement(editedBy);
        ps.executeUpdate(editedBy);

        // table 9
        String producedBy = "CREATE TABLE IF NOT EXISTS ProducedBy " +
                "( personID INTEGER not null, " +
                " movieID INTEGER not null, " +
                " PRIMARY KEY(personID, movieID)," +
                " FOREIGN KEY (personID) REFERENCES Person(personID)," +
                " FOREIGN KEY (movieID) REFERENCES movie(movieID) )";
        ps = conn.prepareStatement(producedBy);
        ps.executeUpdate(producedBy);

        // table 10
        String writtenBy = "CREATE TABLE IF NOT EXISTS WrittenBy " +
                "( personID INTEGER not null, " +
                " movieID INTEGER not null, " +
                " PRIMARY KEY(personID, movieID)," +
                " FOREIGN KEY (personID) REFERENCES Person(personID)," +
                " FOREIGN KEY (movieID) REFERENCES movie(movieID) )";
        ps = conn.prepareStatement(writtenBy);
        ps.executeUpdate(writtenBy);
        ps.close();
        conn.close();

    }

    /**
     * This method loads the person table
     * @throws Exception
     */
    public static void insertPersonTable(String url,String user,String pwd, String pathToData) throws Exception {
        InputStream gzip = new GZIPInputStream(new FileInputStream(pathToData +"\\name.basics.tsv.gz"));
        Scanner scanner1 = new Scanner(gzip, "UTF-8");
        long start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("USE imdb");
        conn.setAutoCommit(false);
        String tryquery = "INSERT INTO Person " +
                "(personID, personName, birthYear, deathYear)" +
                " VALUES (?, ?, ?, ?)";
        PreparedStatement ps = conn.prepareStatement(tryquery);
        // ignores first line of input file
        scanner1.nextLine();
        int count = 1;
        int totalcount = 0;
        while (scanner1.hasNextLine()) {
            if (totalcount=200001){
                break;
            }
            // if (scanner1.hasNextLine()) {
            String[] arrofStr = scanner1.nextLine().split("\\t");
            ps.setInt(1, Integer.parseInt(arrofStr[0].replace("nm", "")));
            ps.setString(2, arrofStr[1]);
            if (!arrofStr[2].contains("N")) {
                ps.setInt(3, Integer.parseInt(arrofStr[2]));
            }else {
                ps.setInt(3, 0);
            }
            if (!arrofStr[3].contains("N")) {
                ps.setInt(4, Integer.parseInt(arrofStr[3]));
            }else {
                ps.setInt(4, 0);
            }
            count++;
            ps.addBatch();
            if (count % 100000 == 0) {
                ps.executeBatch();
                conn.commit();
                count = 0;
            }
            //    }
        }
        if (count > 0) {
            ps.executeBatch();
            conn.commit();
        }
        ps.close();
        stmt.close();
        conn.close();
        long timetaken = ((System.currentTimeMillis() - start) / 60000) + 1;
        System.out.println(timetaken + " minutes taken for person table ");
    }

    /**
     * This method loads the movie table from the file
     * @throws Exception
     */
    public static void insertMovieTable(String url,String user,String pwd,String pathToData) throws Exception {

        InputStream gzip = new GZIPInputStream(new FileInputStream(pathToData+"\\title.basics.tsv.gz"));
        Scanner scanner = new Scanner(gzip, "UTF-8");
        long start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("USE imdb");
        conn.setAutoCommit(false);

        String tryquery = "INSERT IGNORE INTO movie(movieID, title, releaseYear, runtime)" +
                " VALUES (?, ?, ?, ?)";

        PreparedStatement ps = conn.prepareStatement(tryquery);

        // ignores first line of input file
        scanner.nextLine();
        int count = 0;
        while (scanner.hasNextLine()) {

            String[] arrofStr = scanner.nextLine().split("\\t");
            if (arrofStr[1].equals("short") || arrofStr[1].equals("movie") || arrofStr[1].equals("tvMovie") || arrofStr[1].equals("tvShort")) {
                ps.setInt(1, Integer.parseInt(arrofStr[0].replace("tt", "")));
                ps.setString(2, arrofStr[3]);
                if (!arrofStr[5].contains("N")) {
                    ps.setInt(3, Integer.parseInt(arrofStr[5]));
                }
                if (!arrofStr[7].contains("N")) {
                    ps.setInt(4, Integer.parseInt(arrofStr[7]));
                }
            }
            count++;
            ps.addBatch();
            if (count % 10000 == 0) {
                ps.executeBatch();
                conn.commit();
                count = 0;
            }
        }
        if (count > 0) {
            ps.executeBatch();
            conn.commit();
        }
        ps.close();
        stmt.close();
        conn.close();
        scanner.close();
        long timetaken = ((System.currentTimeMillis() - start) / 60000) + 1;
        System.out.println(timetaken + " minutes taken for movie table without ratings");
    }

    /**
     * This method updates movie table with ratings of respective movies
     * @throws Exception
     */
    public static void insertRatings(String url,String user,String pwd,String pathToData) throws Exception {

        InputStream gzip1 = new GZIPInputStream(new FileInputStream(pathToData+"\\title.ratings.tsv.gz"));
        Scanner scanner1 = new Scanner(gzip1, "UTF-8");
        long start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("USE imdb");
        conn.setAutoCommit(false);
        String tryquery1 = "UPDATE movie SET rating = ?, numberOfVotes = ? WHERE movieID = ? ";

        PreparedStatement ps = conn.prepareStatement(tryquery1);
        System.out.println("Entered ratings file");
        // ignores first line of input file
        scanner1.nextLine();
        int count = 0;
        while (scanner1.hasNextLine()) {
            String[] arrofStr1 = scanner1.nextLine().split("\\t");
            ps.setInt(3, Integer.parseInt(arrofStr1[0].replace("tt", "")));
            ps.setFloat(1, Float.parseFloat(arrofStr1[1]));
            ps.setInt(2, Integer.parseInt(arrofStr1[2]));
            count++;
            ps.addBatch();
            if (count % 10000 == 0) {
                ps.executeBatch();
                conn.commit();
                count = 0;
            }

        }
        if (count > 0) {
            ps.executeBatch();
            conn.commit();
        }
        scanner1.close();
        ps.close();
        conn.close();

        long timetaken = ((System.currentTimeMillis() - start) / 60000) + 1;
        System.out.println(timetaken + " minutes taken for ratings table ");
    }

    /**
     * This method loads the genre table
     * @throws Exception
     */
    public static void insertGenre(String url,String user,String pwd,String pathToData) throws Exception {
        long start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        System.out.println("Entered genre file");
        genre.put("Documentary", 1);
        genre.put("Short", 2);
        genre.put("Animation", 3);
        genre.put("Comedy", 4);
        genre.put("Romance", 5);
        genre.put("Sport", 6);
        genre.put("Action", 7);
        genre.put("News", 8);
        genre.put("Drama", 9);
        genre.put("Fantasy", 10);
        genre.put("Horror", 11);
        genre.put("Biography", 12);
        genre.put("Music", 13);
        genre.put("War", 14);
        genre.put("Crime", 15);
        genre.put("Western", 16);
        genre.put("Family", 17);
        genre.put("Adventure", 18);
        genre.put("History", 19);
        genre.put("Mystery", 20);
        genre.put("Adult", 21);
        genre.put("Sci-Fi", 22);
        genre.put("Thriller", 23);
        genre.put("Musical", 24);
        genre.put("Film-Noir", 25);
        genre.put("Game-Show", 26);
        genre.put("Talk-Show", 27);
        genre.put("Reality-TV", 28);
        genre.put("\\N", 29);

        for (Map.Entry mapElement : genre.entrySet()) {
            stmt.executeUpdate("INSERT INTO genre (genreID, genreName) VALUES(" + mapElement.getValue() + ",'" + mapElement.getKey() + "');");

        }
        stmt.close();
        conn.close();
        long timetaken = ((System.currentTimeMillis() - start) / 60000) + 1;
        System.out.println(timetaken + " minutes taken for genre table ");
    }

    /**
     * This method inserts values
     * in all the relationship entity sets
     * e.g actedIN, DirectedBy etc
     */
    public static void insertInProfessions(String url,String user,String pwd,String pathToData) {
        Connection actor, composer, editor, director, producer, writer;
        PreparedStatement psA, psC, psE, psD, psP, psW;
        try {
            actor = DriverManager.getConnection(url, user, pwd);
            composer = DriverManager.getConnection(url, user, pwd);
            director = DriverManager.getConnection(url, user, pwd);
            editor = DriverManager.getConnection(url, user, pwd);
            producer = DriverManager.getConnection(url, user, pwd);
            writer = DriverManager.getConnection(url, user, pwd);
            actor.setAutoCommit(false);
            composer.setAutoCommit(false);
            director.setAutoCommit(false);
            editor.setAutoCommit(false);
            producer.setAutoCommit(false);
            writer.setAutoCommit(false);
            InputStream gzipStream = new GZIPInputStream(new FileInputStream(
                    pathToData+"\\title.principals.tsv.gz"));
            Scanner sc = new Scanner(gzipStream, "UTF-8");
            String actQ = "INSERT IGNORE INTO ActedIN(personID, movieID) SELECT personID, movieID FROM Person, Movie WHERE personID=? AND movieID = ?";
            String comQ = "INSERT IGNORE INTO ComposedBy(personID, movieID) SELECT personID, movieID FROM Person, Movie WHERE personID=? AND movieID = ?";
            String dirQ = "INSERT IGNORE INTO DirectedBy(personID, movieID) SELECT personID, movieID FROM Person, Movie WHERE personID=? AND movieID = ?";
            String ediQ = "INSERT IGNORE INTO EditedBy(personID, movieID) SELECT personID, movieID FROM Person, Movie WHERE personID=? AND movieID = ?";
            String proQ = "INSERT IGNORE INTO ProducedBy(personID, movieID) SELECT personID, movieID FROM Person, Movie WHERE personID=? AND movieID = ?";
            String wriQ = "INSERT IGNORE INTO WrittenBy(personID, movieID) SELECT personID, movieID FROM Person, Movie WHERE personID=? AND movieID = ?";
            psA = actor.prepareStatement(actQ);
            psC = composer.prepareStatement(comQ);
            psD = director.prepareStatement(dirQ);
            psE = editor.prepareStatement(ediQ);
            psP = producer.prepareStatement(proQ);
            psW = writer.prepareStatement(wriQ);
            int num_act = 0, num_dir = 0, num_com = 0, num_edi = 0, num_pro = 0, num_wri = 0;
            long start = System.currentTimeMillis();
            //ignoring first line
            sc.nextLine();
            while (sc.hasNextLine()) {
                String[] arrofStr = sc.nextLine().split("\\t");
                if (arrofStr[3].equals("actor") || arrofStr[3].equals("actress") || arrofStr[3].equals("self")) {
                    psA.setInt(2, Integer.parseInt(arrofStr[0].replace("tt", "")));
                    psA.setInt(1, Integer.parseInt(arrofStr[2].replace("nm", "")));
                    num_act++;
                    psA.addBatch();
                } else if (arrofStr[3].equals("composer")) {
                    psC.setInt(2, Integer.parseInt(arrofStr[0].replace("tt", "")));
                    psC.setInt(1, Integer.parseInt(arrofStr[2].replace("nm", "")));
                    num_com++;
                    psC.addBatch();
                } else if (arrofStr[3].equals("director")) {
                    psD.setInt(2, Integer.parseInt(arrofStr[0].replace("tt", "")));
                    psD.setInt(1, Integer.parseInt(arrofStr[2].replace("nm", "")));
                    num_dir++;
                    psD.addBatch();
                } else if (arrofStr[3].equals("editor")) {
                    psE.setInt(2, Integer.parseInt(arrofStr[0].replace("tt", "")));
                    psE.setInt(1, Integer.parseInt(arrofStr[2].replace("nm", "")));
                    num_edi++;
                    psE.addBatch();
                } else if (arrofStr[3].equals("producer")) {
                    psP.setInt(2, Integer.parseInt(arrofStr[0].replace("tt", "")));
                    psP.setInt(1, Integer.parseInt(arrofStr[2].replace("nm", "")));
                    num_pro++;
                    psP.addBatch();
                } else if (arrofStr[3].equals("writer")) {
                    psW.setInt(2, Integer.parseInt(arrofStr[0].replace("tt", "")));
                    psW.setInt(1, Integer.parseInt(arrofStr[2].replace("nm", "")));
                    num_wri++;
                    psW.addBatch();
                }
                if (num_act % 10000 == 0) {
                    psA.executeBatch();
                    actor.commit();
                    num_act = 0;
                }
                if (num_com % 10000 == 0) {
                    psC.executeBatch();
                    composer.commit();
                    num_com = 0;
                }
                if (num_dir % 10000 == 0) {
                    psD.executeBatch();
                    director.commit();
                    num_dir = 0;
                }
                if (num_edi % 10000 == 0) {
                    psE.executeBatch();
                    editor.commit();
                    num_edi = 0;
                }
                if (num_pro % 10000 == 0) {
                    psP.executeBatch();
                    producer.commit();
                    num_pro = 0;
                }
                if (num_wri % 10000 == 0) {
                    psW.executeBatch();
                    writer.commit();
                    num_wri = 0;
                }
            }
            if (num_act > 0) {
                psA.executeBatch();
                actor.commit();
            }
            if (num_com > 0) {
                psC.executeBatch();
                composer.commit();
            }
            if (num_dir > 0) {
                psD.executeBatch();
                director.commit();
            }
            if (num_edi > 0) {
                psE.executeBatch();
                editor.commit();
            }
            if (num_pro > 0) {
                psP.executeBatch();
                producer.commit();
            }
            if (num_wri > 0) {
                psW.executeBatch();
                writer.commit();
            }
            psA.close();
            psC.close();
            psD.close();
            psE.close();
            psP.close();
            psW.close();
            actor.close();
            composer.close();
            director.close();
            editor.close();
            producer.close();
            writer.close();
            sc.close();
            long timetaken = (System.currentTimeMillis() - start) / (60000);
            System.out.println(timetaken + " minutes, releasing resources");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Thid method maps all movies to their
     * respective genres
     * @throws Exception
     */
    public static void insertHasGenre(String url,String user,String pwd,String pathToData) throws Exception {
        InputStream gzip1 = new GZIPInputStream(new FileInputStream(pathToData+"\\title.basics.tsv.gz"));
        Scanner scanner1 = new Scanner(gzip1, "UTF-8");
        long start = System.currentTimeMillis();
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("USE imdb");
        conn.setAutoCommit(false);

        String tryquery1 = "INSERT INTO HasGenre (genreID, movieID) " +
                "SELECT genreID, movieID FROM genre, movie WHERE genreID = ? AND movieID = ?";
        PreparedStatement ps = conn.prepareStatement(tryquery1);
        System.out.println("Entered HAS Genre file");
        // ignores first line of input file
        scanner1.nextLine();
        int count = 0;
        while (scanner1.hasNextLine()) {
            String[] arrofStr1 = scanner1.nextLine().split("\\t");
            String[] genres = arrofStr1[8].split(",");
            for (String g : genres) {
                ps.setInt(1, genre.get(g));
                ps.setInt(2, Integer.parseInt(arrofStr1[0].replace("tt", "")));
                count++;
                ps.addBatch();

            }
            if (count % 10000 == 0) {
                ps.executeBatch();
                conn.commit();
                count = 0;
            }
        }
        if (count > 0) {
            ps.executeBatch();
            conn.commit();
        }
        scanner1.close();
        stmt.close();
        ps.close();
        conn.close();
        long timetaken = ((System.currentTimeMillis() - start) / 60000) + 1;
        System.out.println(timetaken + " minutes taken for hasGenre!!! ");
    }
}