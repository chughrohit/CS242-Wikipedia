import java.io.IOException;

import java.nio.file.Paths;
import java.io.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.FileReader;
import java.util.Iterator;
import org.json.simple.parser.*;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.time.*;

public class WikiIndexer {

    public static void main(String[] args) throws IOException, ParseException {



        String index_path = "D:\\Anish\\MS\\QTR2\\IR\\lucene\\indices\\wiki_index";
        Directory dir =  FSDirectory.open(Paths.get(index_path));
        Analyzer analyzer =  new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, iwc);

        //Starting point for timer
        Instant start = Instant.now();
        Instant finish;
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();

        File directory = new File("D:\\Anish\\MS\\QTR2\\IR\\data_lat\\data");

        for (File file : directory.listFiles()) {

            System.out.println(file.getName());

            int insertCounter = 0;
            try (BufferedReader reader = new BufferedReader (new FileReader(file))) {
                String line = reader.readLine();
                while (line != null) {
                    insertCounter++;
                    if(insertCounter%10000==0){

                        finish = Instant.now();
 
                        System.out.println("Number of documents: "+insertCounter+" "+Duration.between(start, finish).toMillis());
                    } 

                    //Read JSON file
                    JSONObject obj = (JSONObject) jsonParser.parse(line);
        

                    //JSONArray employeeList = (JSONArray) obj;
                    //System.out.println(employeeList);

                    parseObject(obj , writer );

                    // try{
                    // //Iterate over employee array
                    // employeeList.forEach( emp -> parseObject( (JSONObject) emp,writer ) );


                    // }
                    line =  reader.readLine();

                }
            } catch (Exception e) {}

            // }

            // catch (Exception e) {
            //     e.printStackTrace();
            //     System.out.println(e + "");
            // }

        }





        writer.close();

        // } catch (FileNotFoundException e) {
        //     e.printStackTrace();

        //catch (ParseException e) {
        //     e.printStackTrace();
        // }

        // Document doc = new Document();

        // doc.add(new TextField("title","Dark Knight Rises", Field.Store.YES));
        // doc.add(new TextField("director","C. Nolan", Field.Store.YES));
        //writer.addDocument(doc);
    }





    public static void parseObject(JSONObject obj, IndexWriter writer) {

//System.out.println(obj.get("title"));

        try {
            Document doc = new Document();
            doc.add(new TextField("title", obj.get("title").toString(), Field.Store.YES));
            doc.add(new TextField("text", obj.get("text").toString(), Field.Store.NO));
            doc.add(new TextField("url", obj.get("url").toString(), Field.Store.YES));

            //System.out.println(obj.get("title")+ "| "+obj.get("url"));
            writer.addDocument(doc);
        } catch (Exception e) {}



    }

    public static void insertDocument(IndexWriter idx) {

        Document doc = new Document();
        //doc.add(new TextField(field,content, Field.Store.YES));

    }

}