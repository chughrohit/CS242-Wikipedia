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
import org.json.simple.parser.*;

import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.io.*;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.time.*;

public class WikiIndexer {

    public static void main(String[] args) throws IOException, ParseException {


        //Param: Where Lucene index is to be stored
        String index_path = args[0];
        //String index_path = "D:\\Anish\\MS\\QTR2\\IR\\lucene\\indices\\wiki_index";

        //Param: Where the dataset is located on local drive
        File directory = new File(args[1]);
        //File directory = new File("D:\\Anish\\MS\\QTR2\\IR\\data_lat\\data");

        // Load the index directory
        Directory dir =  FSDirectory.open(Paths.get(index_path));

        // Initialize a standard Analyer
        Analyzer analyzer =  new StandardAnalyzer();

        //Indexwriter setup code
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, iwc);

        //Time logging objects
        Instant start = Instant.now();
        Instant finish;


        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();



        for (File file : directory.listFiles()) {

            System.out.println("Processing " + file.getName() + "...");

            int documentCounter = 0;
            try (BufferedReader reader = new BufferedReader (new FileReader(file))) {

                String line = reader.readLine();
                while (line != null) {

                    documentCounter++;
                    if (documentCounter % 10000 == 0) {

                        finish = Instant.now();
                        System.out.println(documentCounter + " documents processed in" + Duration.between(start, finish).toMillis() / 1000 + " seconds");
                    }

                    //Read the JSON object present in current line
                    JSONObject obj = (JSONObject) jsonParser.parse(line);

                    //Pass the JSON object to be indexed in Lucene
                    parseObject(obj , writer );

                    //Read the next line in the files
                    line =  reader.readLine();

                }
            } catch (Exception e) {}

        }

        writer.close();

    }

    public static void parseObject(JSONObject obj, IndexWriter writer) {



        try {

            Document doc = new Document();
            doc.add(new TextField("title", obj.get("title").toString(), Field.Store.YES));
            doc.add(new TextField("text", obj.get("text").toString(), Field.Store.NO));
            doc.add(new TextField("url", obj.get("url").toString(), Field.Store.YES));


            writer.addDocument(doc);

        } catch (Exception e) {

        }
    }

}