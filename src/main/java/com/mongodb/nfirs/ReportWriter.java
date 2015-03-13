package com.mongodb.nfirs;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.stream.JsonWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ReportWriter {
    
    private int year;
    private String state;
    private String county;
    private DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("MM/dd/YYYY");
    
    MongoClient mongoClient;
    private DBCollection incidentCountyMonthly;
    private DBCollection incidentStateMonthly;
    private DBCollection incidentNationalMonthly;
    
    JsonWriter writer = new JsonWriter(new OutputStreamWriter(System.out));
    
    
    
    public ReportWriter(int year, String state, String county) throws UnknownHostException {
        this.year = year;
        this.state = state;
        this.county = county;
        mongoClient = new MongoClient("localhost", 27017);
        DB db = mongoClient.getDB("nfirs");
        incidentCountyMonthly = db.getCollection("IncidentCountyMonthly");
        incidentStateMonthly = db.getCollection("IncidentStateMonthly");
        incidentNationalMonthly = db.getCollection("IncidentNationalMonthly");
    }
    
    private List<DBObject> getGenericTypeCountPercent(DBCollection coll, DBObject query) { 
        List<DBObject> results = new ArrayList<DBObject>();
        int totalCount = 0;
        DBCursor cursor = coll.find(query);
        while (cursor.hasNext()) {
            DBObject row = cursor.next();
            double count = (Double)row.get("count");
            totalCount += count;
            results.add(row);
        }
        for (DBObject row : results) {
            double count = (Double)row.get("count");
            double percentOfTotal = count / totalCount * 100;
            row.put("percent_of_whole", percentOfTotal);
        }
        return results;
    }
    
    private List<DBObject> getIncidentsByCounty(int month) {
        List<DBObject> incidents = new ArrayList<DBObject>();
        DBObject query = new BasicDBObject();
        query.put("state", state);
        query.put("county", county);
        query.put("year", year);
        query.put("month", month);
        return getGenericTypeCountPercent(incidentCountyMonthly, query);
    }
    
    private List<DBObject> getIncidentsByState(int month) {
        List<DBObject> incidents = new ArrayList<DBObject>();
        DBObject query = new BasicDBObject();
        query.put("state", state);
        query.put("year", year);
        query.put("month", month);
        return getGenericTypeCountPercent(incidentStateMonthly, query);
    }
    
    private List<DBObject> getIncidentsNational(int month) {
        List<DBObject> incidents = new ArrayList<DBObject>();
        DBObject query = new BasicDBObject();
        query.put("year", year);
        query.put("month", month);
        return getGenericTypeCountPercent(incidentNationalMonthly, query);
    }
    
    private void writeGenericTypeValuePercent(String rootName, List<DBObject> incidents) throws IOException {
        writer.name(rootName).beginObject();
        writer.name("incident_types").beginArray();
        
        
        for (DBObject incident : incidents) {
            writer.beginObject(); // incident
            writer.name("type").value((String)incident.get("type"));
            writer.name("value").value(((Double)incident.get("count")).intValue());
            writer.name("percent_of_whole").value((double)incident.get("percent_of_whole"));
            writer.endObject(); // incident
        }
       
        writer.endArray(); // incident_types
        
        writer.name("failure_types").beginArray();
        // TODO
        writer.endArray(); // failure_types
        writer.endObject(); // rootName
    }
    
    private void writeMetroData(int month) throws IOException {
        List<DBObject> incidents = getIncidentsByCounty(month);
        writeGenericTypeValuePercent("metro_data", incidents);
    }
    
    private void writeStateData(int month) throws IOException {
        List<DBObject> incidents = getIncidentsByState(month);
        writeGenericTypeValuePercent("state_data", incidents);
    }
    
    private void writeNationalData(int month) throws IOException {
        List<DBObject> incidents = getIncidentsNational(month);
        writeGenericTypeValuePercent("national_data", incidents);
    }
    
    public void writeReport() throws IOException {
        
        writer.setIndent("    ");
        writer.beginObject();
        writer.name("name").value(county);
        writer.name("state").value(state);
        
        DateTime currentDate = new DateTime();
        String currentMonth = currentDate.toString("MMMM");
        
        writer.name("current_month").value(currentMonth);
        writer.name("current_year").value(currentDate.getYear());
        writer.name("current_date").value(dateTimeFormatter.print(currentDate));
        
        writer.name("years").beginArray();
        writer.beginObject();
        writer.name("year").value(year);
        writer.name("month_periods").beginArray();
        
        for (int month = 1; month <= 12; month++) {
            writer.beginObject(); // month
            writer.name("month").value(month);
            writeMetroData(month);
            writeStateData(month);
            writeNationalData(month);
            writer.endObject(); // month
        }
        
        
        writer.endArray(); // month_periods
        
        writer.endObject(); // year
        writer.endArray(); // years
        
        writer.endObject(); // root
        writer.close();
    }



    private static void usage() {
        System.err.println("usage: ReportWriter <year> <state> <county>");
        
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            usage();
            System.exit(-1);
        }
        int year = Integer.parseInt(args[0]);
        
        
        ReportWriter writer = new ReportWriter(year, args[1], args[2]);
        writer.writeReport();
    }



    

}
