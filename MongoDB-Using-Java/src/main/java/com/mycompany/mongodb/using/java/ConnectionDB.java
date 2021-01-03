package com.mycompany.mongodb.using.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Nano
 */
public class ConnectionDB {

    public static Mongo mongo;
    public static DB db;
    public static DBCollection table;
    public static DBCursor cursor;
    public static BasicDBObject searchQuery;
    public static Object json;
    public static ObjectMapper objectMapper = new ObjectMapper();

    public static void conn() {
        mongo = new Mongo("localhost", 27017);
        db = mongo.getDB("school");
    }

    public static DBCollection connIsExists(String collName) {
        conn();

        if (!db.collectionExists(collName)) {
            db.createCollection(collName, null);
        }
        table = db.getCollection(collName);
        System.out.println("Connection Successfull");
        //table.drop();
        return table;
    }

    public static void getDataBaseNames() {
        System.out.println("----- Getting Database Names -----");
        List dbs = mongo.getDatabaseNames();
        for (Object alB : dbs) {
            System.out.println(alB);
        }
    }

    public static void getCollectionNames() {
        table = db.getCollection("post");
        System.out.println("----- Getting Collections -----");
        Set colls = db.getCollectionNames();
        for (Object s : colls) {
            System.out.println(s);
        }
    }

    public static void GetASingleDocument(String collName, String Key, String Value) throws IOException {
        System.out.println("----- Get A Single Document -----");
        table = db.getCollection(collName);
        searchQuery = new BasicDBObject(Key, Value);

        cursor = table.find(searchQuery);
        while (cursor.hasNext()) {
            json = objectMapper.readValue(cursor.next().toString(), Object.class);
            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

        }
    }

    public static void GetAllDataFromMongoDBWithJSonViewFormat(String collName) throws IOException {
        System.out.println("----- Getting All Data From Collection -----");

        cursor = db.getCollection(collName).find();
        while (cursor.hasNext()) {
            json = objectMapper.readValue(cursor.next().toString(), Object.class);
            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

        }

    }

    public static void GetSpecificDataFromMongoDBWithJSonViewFormat(String collName) throws IOException {
        System.out.println("----- Getting All Data From Collection -----");

        cursor = db.getCollection(collName).find();
        while (cursor.hasNext()) {
            json = objectMapper.readValue(cursor.next().toString(), Object.class);
            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

        }

    }

    public static void insertDataToMongoDB(String collName) {
        table = db.getCollection(collName);
        searchQuery = new BasicDBObject("StudentNo", "2").
                append("FirstName", "Selcuk").
                append("SurName", "GENC").
                append("Age", "45");

        table.insert(searchQuery);

//        searchQuery.put("name", "Joe Smith");
//        searchQuery.put("age", 25);
//        searchQuery.put("createdDate", new Date()); 
    }

    public static void updateDataToMongoDB(String collName, String Key, String Value, String NewValue) {
        table = db.getCollection(collName);
        searchQuery = new BasicDBObject();
        searchQuery.put(Key, Value);

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put(Key, NewValue);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        table.update(searchQuery, updateObj);
    }

    public static void deleteDataToMongoDB(String collName, String Key, String Value) {
        table = db.getCollection(collName);
        searchQuery = new BasicDBObject();
        searchQuery.put(Key, Value);
        table.remove(searchQuery);
    }

    public static void listSpecificDataToMongoDB(String collName, String Key, String Value) throws IOException {
        table = db.getCollection(collName);
        searchQuery = new BasicDBObject();
        searchQuery.put(Key, Value);
        cursor = table.find(searchQuery);
        while (cursor.hasNext()) {
            json = objectMapper.readValue(cursor.next().toString(), Object.class);
            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

        }
    }

    public static void main(String[] args) throws IOException {
        connIsExists("post");
        //getDataBaseNames();
        //getCollectionNames();
        //GetAllDataFromMongoDBWithJSonViewFormat("students");

        //GetASingleDocument("students", "StudentNo", "1");
        //insertDataToMongoDB("students");
        //updateDataToMongoDB("students","StudentNo", "1", "7");
        //deleteDataToMongoDB("students","StudentNo", "1");
        listSpecificDataToMongoDB("students", "StudentNo", "1");
    }

}
