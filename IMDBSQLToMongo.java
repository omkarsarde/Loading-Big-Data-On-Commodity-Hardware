
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author : Omkar Sarde
 * @version : 1.0
 */

/**
 * This program connects to MySQL database using jdbc driver and
 * Migrates it to a MongoDB database.
 */

class IMDBSQLToMongo {
    String s_url,s_usr,s_pwd,m_url,m_database;
    IMDBSQLToMongo(String sql_url,String sql_usr,String sql_pwd,String mongo_url,String mongo_database){
        s_url = sql_url;
        s_usr = sql_usr;
        s_pwd = sql_pwd;
        m_url = mongo_url;
        m_database = mongo_database;
    }

    /**
     * Get client
     * @param u Client URI
     * @return
     */
    static MongoClient getClient(String u){
        MongoClient client = null;
        if(u.equals("None")){
            client = new MongoClient();

        }else {
            client = new MongoClient(new MongoClientURI(u));
        }
        return client;
    }

    /**
     * Migrate movies table
     * @return boolean true or false based on operation completion status
     */
    boolean migrate_movies(){

        try {
            Connection connection = DriverManager.getConnection(s_url,s_usr,s_pwd);
            MongoClient mongoClient = getClient(m_url);
            MongoDatabase mongoDatabase = mongoClient.getDatabase(m_database);
            String query = "select * from movie";
            MongoCollection<Document> collection = mongoDatabase.getCollection("Movies");
            Statement statement = connection.createStatement();
            statement.setFetchSize(10000);
            ResultSet resultSet = statement.executeQuery(query);
            while(resultSet.next()){
                int id = resultSet.getInt("id");
                String title = resultSet.getString("title");
                int releaseYear = resultSet.getInt("releaseYear");
                int runtime = resultSet.getInt("runtime");
                Float rating = resultSet.getFloat("rating");
                int numberOfVotes = resultSet.getInt("numberOfVotes");
                List<String> genres = new LinkedList<>();
                Document document = new Document();
                document.put("_id",id);
                document.put("title",title);
                if(releaseYear != 0){
                    document.put("releaseYear",releaseYear);
                }
                if(runtime != 0){
                    document.put("runtime",runtime);
                }
                if(rating !=0){
                    document.put("rating",rating);
                }
                if(numberOfVotes !=0){
                    document.put("numberOfVotes",numberOfVotes);
                }
                document.put("genres",genres);
                collection.insertOne(document);
            }
            statement.close();
            connection.close();
            mongoClient.close();
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /***
     * Populate Genres table
     * @param migrated_movies flag to check if movies are migrated as genres are dependent on movies
     * @return
     */
    boolean populate_genres(boolean migrated_movies){

        if(migrated_movies) {
            try {
                Connection connection = DriverManager.getConnection(s_url, s_usr, s_pwd);
                MongoClient mongoClient = getClient(m_url);
                MongoDatabase mongoDatabase = mongoClient.getDatabase(m_database);
                MongoCollection<Document> collection = mongoDatabase.getCollection("Movies");
                String query = "SELECT hasgenre.movieId, genre.name ";
                query +=" from genre join hasgenre on genre.id = hasgenre.genreId ";
                query+=" ORDER BY hasgenre.movieId ASC";
                Statement statement = connection.createStatement();
                statement.setFetchSize(10000);
                ResultSet resultSet = statement.executeQuery(query);
                while(resultSet.next()){
                    int m_id = resultSet.getInt("movieId");
                    String current_genre = resultSet.getString("name");
                    if(current_genre != "\\N"){
                        collection.updateOne(Filters.eq("_id",m_id), Updates.addToSet("genres",current_genre));
                    }
                }
                statement.close();
                connection.close();
                mongoClient.close();
                return true;
            }catch (Exception e){
                e.printStackTrace();
                return false;
            }
        }
        return false;
    }

    /***
     * Migrate person table
     * @param populated_genres boolean to check if genres are populated
     * @return
     */
    boolean migrate_people(boolean populated_genres){
        if(populated_genres){
            try {
                Connection connection = DriverManager.getConnection(s_url,s_usr,s_pwd);
                MongoClient mongoClient = getClient(m_url);
                MongoDatabase mongoDatabase = mongoClient.getDatabase(m_database);
                String query = "select * from person";
                MongoCollection<Document> collection = mongoDatabase.getCollection("People");
                Statement statement = connection.createStatement();
                statement.setFetchSize(10000);
                ResultSet resultSet = statement.executeQuery(query);
                while (resultSet.next()){
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    int birthYear = resultSet.getInt("birthYear");
                    int deathYear = resultSet.getInt("deathYear");
                    Document document = new Document();
                    List<String> actor = new ArrayList<>();
                    List<String> composer = new ArrayList<>();
                    List<String> director = new ArrayList<>();
                    List<String> editor = new ArrayList<>();
                    List<String> producer = new ArrayList<>();
                    List<String> writer = new ArrayList<>();
                    document.put("_id",id);
                    document.put("name",name);
                    if(birthYear != 0){
                        document.put("birthYear",birthYear);
                    }
                    if(deathYear != 0){
                        document.put("deathYear",deathYear);
                    }
                    document.put("actor",actor);
                    document.put("composer",composer);
                    document.put("director",director);
                    document.put("editor",editor);
                    document.put("producer",producer);
                    document.put("writer",writer);
                    collection.insertOne(document);
                }
                statement.close();
                connection.close();
                mongoClient.close();
                return true;
            }catch (Exception e){
                e.printStackTrace();
                return false;
            }
        }return false;
    }

    /***
     * Migrate people occupations table
     * @param migrated_people boolean to check if person table is migrated
     * @param array_to_migrate Occupation of the person
     * @return
     */
    boolean populate_people(boolean migrated_people, String array_to_migrate){
        if(migrated_people){
            try {
                Connection connection = DriverManager.getConnection(s_url, s_usr, s_pwd);
                MongoClient mongoClient = getClient(m_url);
                MongoDatabase mongoDatabase = mongoClient.getDatabase(m_database);
                MongoCollection<Document> collection = mongoDatabase.getCollection("People");
                String query = "";
                if(array_to_migrate.equals("actor")){
                    query+="select actedIn.personId, movie.id from movie ";
                    query+=" join actedIn on movie.id = actedin.movieId order by movie.id  ASC ";

                }else  if(array_to_migrate.equals("composer")){
                    query+="select composedBy.personId, movie.id from movie ";
                    query+=" join composedBy on movie.id = composedBy.movieId order by movie.id  ASC";

                }else  if(array_to_migrate.equals("director")){
                    query+="select directedBy.personId, movie.id from movie ";
                    query+=" join directedBy on movie.id = directedBy.movieId order by movie.id  ASC";

                }
                else  if(array_to_migrate.equals("editor")){
                    query+="select editedBy.personId, movie.id from movie ";
                    query+=" join editedBy on movie.id = editedBy.movieId order by movie.id  ASC";

                }
                else  if(array_to_migrate.equals("producer")){
                    query+="select producedBy.personId, movie.id from movie ";
                    query+=" join producedBy on movie.id = producedBy.movieId order by movie.id  ASC";

                }else {
                    query+="select writtenBy.personId, movie.id from movie ";
                    query+=" join writtenBy on movie.id = writtenBy.movieId order by movie.id ";

                }
                Statement statement = connection.createStatement();
                statement.setFetchSize(10000);
                ResultSet resultSet = statement.executeQuery(query);
                while (resultSet.next()){
                    int p_id = resultSet.getInt("personId");
                    int m_id = resultSet.getInt("id");
                    collection.updateOne(Filters.eq("_id",p_id),Updates.addToSet(array_to_migrate,m_id));
                }
                statement.close();
                connection.close();
                mongoClient.close();
                return true;
            }catch (Exception e){
                e.printStackTrace();
                return false;
            }
        }
        return false;
    }

    public static void main(String[] args) {
        String s_url = args[0];
        String s_usr = args[1];
        String s_pwd = args[2];
        String m_url = args[3];
        String m_database = args[4];
        IMDBSQLToMongo driver = new IMDBSQLToMongo(s_url,s_usr,s_pwd,m_url,m_database);
        boolean movies = driver.migrate_movies();
        boolean genres = driver.populate_genres(movies);
        boolean people = driver.migrate_people(genres);
        boolean current_array = driver.populate_people(people,"actor");
        current_array = driver.populate_people(current_array,"composer");
        current_array = driver.populate_people(current_array,"director");
        current_array = driver.populate_people(current_array,"editor");
        current_array = driver.populate_people(current_array,"producer");
        current_array = driver.populate_people(current_array,"writer");

    }
}
