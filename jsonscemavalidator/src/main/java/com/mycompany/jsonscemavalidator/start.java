/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.jsonscemavalidator;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.commons.io.FilenameUtils;
import org.bson.Document;

/**
 *
 * @author ma2218
 */
public class start {

    public static void main(String[] args) throws ProcessingException, IOException {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream("C:/Users/ma2218/Documents/NetBeansProjects/jsonscemavalidator/src/main/java/com/mycompany/jsonscemavalidator/config.properties");
            // load a properties file
            prop.load(input);
            // set the properties value
        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        File folder = new File(prop.get("dburl").toString());
        listFilesForFolder(folder);
    }

    public static void listFilesForFolder(final File folder) throws ProcessingException, IOException {
        String dbname = folder.getName();
        System.out.println("DB is=" + dbname);
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase(dbname);

        File targetFile = new File(folder.getPath() + "/" + dbname + "_db_invalid_ids.txt");
        System.out.println("*********file writter file location *****************" + targetFile);
        FileWriter fwri = null;

        for (final File coll_file : folder.listFiles()) {

            if (coll_file.isDirectory()) {
                listFilesForFolder(coll_file);
            } else {
                System.out.println("filename is" + coll_file.getName());

                fwri = new FileWriter(targetFile, true);

                String fileNameWithOutExt = FilenameUtils.removeExtension(coll_file.getName());
                fwri.write(fileNameWithOutExt + "\n");
                 fwri.flush();
                Validationtask(database, coll_file, fwri);
                System.out.println("*******************************************End of file***********************************");
            }
        }
        if (fwri != null) {
            fwri.close();
        }
    }

    public static void Validationtask(MongoDatabase database, File coll, FileWriter fwri) throws ProcessingException, IOException {
        String fileNameWithOutExt = FilenameUtils.removeExtension(coll.getName());
        System.out.println("FILE NAME IS=" + fileNameWithOutExt);
        MongoCollection<Document> collection = database.getCollection(fileNameWithOutExt);
        System.out.println("name of collection is=" + collection.getNamespace());
        FindIterable findIterable = collection.find();
        List<String> invalids = new ArrayList<>();
        Iterator iterator = findIterable.iterator();

        while (iterator.hasNext()) {
            Document plant = (Document) iterator.next();
            System.out.println("--------id of dictionary that is being evaluated is----------" + plant.get("_id"));
            if (ValidationUtils.isJsonValid(coll, plant.toJson())) {
                System.out.println("Valid!");
            } else {
                invalids.add(plant.get("_id").toString());
                System.out.println("NOT valid!");
                fwri.write(plant.get("_id").toString() + "\n");
                fwri.flush();
               // fwri.close();
            }
        }
        System.out.println("INVALIDS ARRAY SIZE IS=" + invalids.size());

    }
}
