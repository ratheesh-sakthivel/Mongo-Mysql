import java.sql.*;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase; 
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class Mongo {
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try{ 
			
				
		      //Movies
			  ArrayList movieid = new ArrayList();
		      ArrayList title = new ArrayList();
		      ArrayList imdbid = new ArrayList();
		      ArrayList year = new ArrayList();
		      
		      //Language	
		      ArrayList langmovieid = new ArrayList();
		      ArrayList language = new ArrayList();
		      ArrayList addition = new ArrayList();
		      
		      //Tablenames
		      String[] tableNames = {"colorinfo","countries","genres","plots","taglines"};

		      		      
	         MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	         DB db = mongoClient.getDB("imdb");
		     System.out.println("Connect to database successfully");
		     //DBCollection movies = db.getCollection("movies");
		     DBCollection movies = db.getCollection("movies");

			Class.forName("com.mysql.jdbc.Driver"); 
		    String connectionURL = "jdbc:mysql://localhost:3306/jmdb?autoReconnect=true&useSSL=false";

			Connection con= DriverManager.getConnection(connectionURL,"root","mysql");  
			  
			Statement stmt=con.createStatement();  
			
			ResultSet resultSet =stmt.executeQuery("select * from movies LIMIT 1000");
			ResultSetMetaData resultSetMeta = resultSet.getMetaData();
			int columnCount = resultSetMeta.getColumnCount();
			
			
			
			while(resultSet.next())
				{
				movieid.add(resultSet.getString(1));
				title.add(resultSet.getString(2));
				year.add(resultSet.getString(3));
				imdbid.add(resultSet.getString(4));
				}
			
			//Language	
			
			
			resultSet = stmt.executeQuery("select * from language LIMIT 1000");
			while(resultSet.next())
			{
				langmovieid.add(resultSet.getString(1));
				language.add(resultSet.getString(2));
				addition.add(resultSet.getString(3));
			}
			for(int i=0;i<langmovieid.size();i++)
				{
				for(int j=0;j<movieid.size();j++)
					{
					if(langmovieid.get(i).equals(movieid.get(j)))
						{
						BasicDBObject newDocument = new BasicDBObject("movieid",movieid.get(j)).append("title",title.get(j))
							.append("Year",year.get(j))
							.append("IMDB ID",imdbid.get(j))
							.append("Language", language.get(i))
							.append("Addition", addition.get(i));
						movies.insert(newDocument);
						}
					}
				}
			
			for(int i=0;i<tableNames.length;i++)
			{
				resultSet = stmt.executeQuery(String.format("select * from %s LIMIT 1000",  tableNames[i]));
				resultSetMeta = resultSet.getMetaData();
				columnCount = resultSetMeta.getColumnCount();
				System.out.println(resultSetMeta.getColumnLabel(2));
				

				while(resultSet.next()){
					

					for (int j = 2; j <= columnCount; j++) {
						BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
						BasicDBObject appendDocument = new BasicDBObject();
						BasicDBObject newDocument = new BasicDBObject();
						appendDocument.append(resultSetMeta.getColumnLabel(j), resultSet.getString(j));
						newDocument.append("$set",appendDocument);
						movies.update(searchQuery,newDocument);
					}
					
				}
			}

			
			resultSet = stmt.executeQuery("select m.movieid,m.actorid,m.as_character,a.sex,a.name from actors as a inner join movies2actors as m on a.actorid=m.actorid order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Actors",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			resultSet = stmt.executeQuery("select m.movieid,c.cinematid,m.addition,c.name from cinematgrs as c inner join movies2cinematgrs as m on c.cinematid=m.cinematid  order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Cinematographers",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			
			resultSet = stmt.executeQuery("select m.movieid,c.composerid,m.addition,c.name from composers as c inner join movies2composers as m on c.composerid = m.composerid order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Composers",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			
			resultSet = stmt.executeQuery("select m.movieid,d.directorid,m.addition,d.name from directors as d inner join movies2directors as m on d.directorid = m.directorid order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {					
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Directors",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			
			resultSet = stmt.executeQuery("select m.movieid,e.editorid,m.addition,e.name from editors as e inner join movies2editors as m on e.editorid = m.editorid order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Editors",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			
			resultSet = stmt.executeQuery("select c.costdesid,m.movieid,m.addition,c.name from costdesigners as c inner join movies2costdes as m on c.costdesid = m.costdesid order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Editors",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			
			
			resultSet = stmt.executeQuery("select m.movieid,p.producerid,m.addition,p.name from producers as p inner join movies2producers as m on p.producerid = m.producerid order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Producers",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			
			resultSet = stmt.executeQuery("select m.movieid,w.writerid,m.addition,w.name from writers as w inner join movies2writers as m on w.writerid = m.writerid order by m.movieid asc limit 1000");
			resultSetMeta = resultSet.getMetaData();
			columnCount = resultSetMeta.getColumnCount();
			System.out.println(resultSetMeta.getColumnLabel(2));
			

			while(resultSet.next()){
				BasicDBObject searchQuery = new BasicDBObject("movieid", resultSet.getString(1));
				BasicDBObject appendDocument = new BasicDBObject();
				BasicDBObject newDocument = new BasicDBObject();
				BasicDBObject update = new BasicDBObject();

				for (int i = 2; i <= columnCount; i++) {
					appendDocument.put(resultSetMeta.getColumnLabel(i), resultSet.getString(i));
					newDocument.put("$push", new BasicDBObject("Writers",appendDocument));
				}
				movies.update(searchQuery,newDocument);

			}
			
			DBCursor cursor = movies.find();
			
			 while (cursor.hasNext()) { 
		            DBObject doc = cursor.next();
		            System.out.println(doc);
		         }
			
			
						cursor.close();
						con.close(); 

						
			}
		
		
		catch(Exception e){ System.out.println(e);}  
			 
		
	}

}
