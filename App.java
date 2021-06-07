package com.ludo.vijay;
import java.io.FileReader;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.util.ArrayList;
 
import java.io.BufferedReader;
import java.io.File;
// parse csv file code
public class App {
	public static  void main(String args[]){
		 String file = "E://Task1_Jun/Ludo.csv";
		 
		List<Document> documents = new ArrayList<Document>();
		try {
					
			
			
			// connection mongo Service
			//MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

						 //Connect to the database 
			MongoDatabase mongoDatabase = mongoClient.getDatabase("users");
			System.out.println("*********************connect to database successfully*********************");
								// Create Collection
			boolean collectionExists = mongoClient.getDatabase("users").listCollectionNames()
				    .into(new ArrayList<String>()).contains("usersort");
			if(collectionExists == true) {
				mongoDatabase.getCollection("usersort").drop();
				 System.out.println ( "*********************Existing Collection Dropped *******************");
											}
				mongoDatabase.createCollection("usersort");
				 System.out.println ( "*********************Creating a collection of success*********************");
				 	// select a collection
			MongoCollection<Document> collection = mongoDatabase.getCollection("usersort");
			
						 System.out.println ( " usersort collection from users database selected successfully");
				
						 
						 
						 
						 
						 FileReader filereader = new FileReader(file);
						  
					        // create csvReader object and skip first Line
					        CSVReader csvReader = new CSVReaderBuilder(filereader)
					                                  .withSkipLines(1)
					                                  .build();
					        List<String[]> allData = csvReader.readAll();
						 	
					        for (int rowIndex=0; rowIndex<allData.size(); rowIndex++) {
					        	String[] row =  allData.get(rowIndex);

					          
					        	 String uid = row[0]; // get the value in the csv assign keywords
									String tx_coins = row[1];
								String total_coins_after = row[3];
								String time_stamp = row[10];
							//	System.out.println();
								
									Document old = collection.find(Filters.eq("_id",  uid)).first();
									if (old!=null) {
										DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:m:ss zzz");
										ZonedDateTime	oldDate = ZonedDateTime.parse(old.get("time_stamp").toString(), inputFormatter);
										
										ZonedDateTime	newDate = ZonedDateTime.parse(time_stamp, inputFormatter);
										if (newDate.compareTo(oldDate) > 0) {
											Document document = new Document (); // create a document
											document.put( "_id",uid); // data into the database
											document.put("tx_coins", tx_coins);
										document.put("total_coins_after", total_coins_after);
											document.put("time_stamp", time_stamp);
											//documents.add(document);
											//collection.insertOne(document);
											UpdateOptions updateOp =  new UpdateOptions();
											updateOp.upsert(true);
											
											collection.updateOne(Filters.eq("_id",  uid), new Document("$set", document), updateOp);
											
										}
										//System.out.println(old.get("time_stamp"));
									} else {
										Document document = new Document (); // create a document
										document.put( "_id",uid); // data into the database
										document.put("tx_coins", tx_coins);
									document.put("total_coins_after", total_coins_after);
										document.put("time_stamp", time_stamp);
										//documents.add(document);
										collection.insertOne(document);
									}
					
					        }
						     
						 	
					       
						 //collection.insertMany(documents); // set the document into the database
						 
						 System.out.println("################ Inserts Succesfull  #################");	
					
		
		
								}
								catch (Exception e){
			e.printStackTrace();
		}
 
 
	}
}
 