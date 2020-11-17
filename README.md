#  Loading Big Data On Commodity Hardware
 What would happen if Amazon Servers had only **128 MB of RAM?** <br> 
 Would such a system would ever be able to load the IMDB dataset ?
 <br> With these questions in mind dataloaders for Extract Transform and Load (ETL) pipelines were created using vanilla Java and JDBC drivers.
 
 # Approach
 **First:** A standalone MySQL (RDBMS) database was created that can use commodity hardware based on 128 MB of RAM as minimum requirement. This is done using the IMDBLoder.java file.
 <br>The code is well documented and requires argumentative input.
 <br>
 **Second:** The other end of spectrum of database systems is the NoSQL world.<br> To develop a MongoDB database similar to the earlier MySQL database use the IMDBSQLToMongo.java which migrates the entire SQL database to No-SQL format.
<br> Extensive care was taken to make both the implementations be unrestricted by hardware requirements.
