package com.mongodb.nfirs;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.stream.JsonWriter;
import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class ReportWriter {
    
    protected static final Logger logger = LoggerFactory.getLogger(ReportWriter.class);
    
    private int reportYear;
    private String state;
    private String county;
    private DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("MM/dd/YYYY");
    
    MongoClient mongoClient;
    private DBCollection incidentCountyMonthly;
    private DBCollection incidentStateMonthly;
    private DBCollection incidentNationalMonthly;
    
    private DBCollection equipmentFailureCountyMonthly;
    private DBCollection equipmentFailureStateMonthly;
    private DBCollection equipmentFailureNationalMonthly;
    
    private DBCollection incidentCountyYtd;
    private DBCollection incidentStateYtd;
    private DBCollection incidentNationalYtd;
    
    private DBCollection equipmentFailureCountyYtd;
    private DBCollection equipmentFailureStateYtd;
    private DBCollection equipmentFailureNationalYtd;
    
    JsonWriter writer;
    
    public ReportWriter(int year, String state, String county) throws IOException {
        
        String countyName = county.replaceAll(" ", "_").toLowerCase();
        
        writer = new JsonWriter(new FileWriter("data/" + countyName + ".json"));
        
        this.reportYear = year;
        this.state = state;
        this.county = county;
        mongoClient = new MongoClient("localhost", 27017);
        DB db = mongoClient.getDB("nfirs");
        incidentCountyMonthly = db.getCollection("IncidentCountyMonthly");
        incidentStateMonthly = db.getCollection("IncidentStateMonthly");
        incidentNationalMonthly = db.getCollection("IncidentNationalMonthly");
        
        equipmentFailureCountyMonthly = db.getCollection("EquipFailure_CookLAHoward_FireEquipItembyCtyMoYR");
        equipmentFailureStateMonthly = db.getCollection("EquipFailbyStateYear_FireDeptStateMoYearItem");
        equipmentFailureNationalMonthly = db.getCollection("EquipmentFailureNationalMonthly");
        
        incidentCountyYtd = db.getCollection("IncidentCountyYtd");
        incidentStateYtd = db.getCollection("IncidentStateYtd");
        incidentNationalYtd = db.getCollection("IncidentNationalYtd");
        
        equipmentFailureCountyYtd = db.getCollection("EquipmentFailureCountyYtd");
        equipmentFailureStateYtd = db.getCollection("EquipmentFailureStateYtd");
        equipmentFailureNationalYtd = db.getCollection("EquipmentFailureNationalYtd");
        
    }
    
    private List<DBObject> getGenericTypeCountPercent(DBCollection coll, DBObject query) { 
        List<DBObject> results = new ArrayList<DBObject>();
        int totalCount = 0;
        DBCursor cursor = coll.find(query);
        logger.debug(coll.getName() + " " + query + " count: " + cursor.count());
        
        while (cursor.hasNext()) {
            DBObject row = cursor.next();
            Double count = getCount(row);
            totalCount += count;
            results.add(row);
        }
        for (DBObject row : results) {
            Double count = getCount(row);
            double percentOfTotal = 0;
            if (count > 0) {
                percentOfTotal = count / totalCount * 100;
            }
            row.put("percent_of_whole", percentOfTotal);
        }
        return results;
    }
    
    private List<DBObject> getByCounty(DBCollection collection, int year, int month) {
        DBObject query = new BasicDBObject();
        query.put("state", state);
        query.put("county", county);
        query.put("year", year);
        query.put("month", month);
        return getGenericTypeCountPercent(collection, query);
    }
    
    private List<DBObject> getByState(DBCollection collection, int year, int month) {
        DBObject query = new BasicDBObject();
        query.put("state", state);
        query.put("year", year);
        query.put("month", month);
        return getGenericTypeCountPercent(collection, query);
    }
    
    private List<DBObject> getNational(DBCollection collection, int year, int month) {
        DBObject query = new BasicDBObject();
        query.put("year", year);
        query.put("month", month);
        return getGenericTypeCountPercent(collection, query);
    }
    
    private void writeGenericList(String listNameJson, List<DBObject> list) throws IOException {
        writer.name(listNameJson).beginArray();
        for (DBObject incident : list) {
            writer.beginObject();
            writer.name("type").value((String)incident.get("type"));
            writer.name("value").value(getCount(incident).intValue());
            writer.name("percent_of_whole").value((double)incident.get("percent_of_whole"));
            writer.endObject();
        }
        writer.endArray();
    }
    
    private void writeGenericTypeValuePercent(String rootName, List<DBObject> incidents, List<DBObject> failureTypes) throws IOException {
        writer.name(rootName).beginObject();
        
        writeGenericList("incident_types", incidents);
        
        writeGenericList("failure_types", failureTypes);
        
        writer.endObject(); // rootName
    }
    
    private void writeMetroData(int year, int month) throws IOException {
        List<DBObject> incidents = getByCounty(incidentCountyMonthly, year, month);
        List<DBObject> failureTypes = getByCounty(equipmentFailureCountyMonthly, year, month);
        writeGenericTypeValuePercent("metro_data", incidents, failureTypes);
    }
    
    private void writeStateData(int year, int month) throws IOException {
        List<DBObject> incidents = getByState(incidentStateMonthly, year, month);
        List<DBObject> failureTypes = getByState(equipmentFailureStateMonthly, year, month);
        writeGenericTypeValuePercent("state_data", incidents, failureTypes);
    }
    
    private void writeNationalData(int year, int month) throws IOException {
        List<DBObject> incidents = getNational(incidentNationalMonthly, year, month);
        List<DBObject> failureTypes = getNational(equipmentFailureNationalMonthly, year, month);
        writeGenericTypeValuePercent("national_data", incidents, failureTypes);
    }
    
    private void writeMetroDataYtd(int year, int month) throws IOException {
        List<DBObject> incidents = getByCounty(incidentCountyYtd, year, month);
        List<DBObject> failureTypes = getByCounty(equipmentFailureCountyYtd, year, month);
        
        writeGenericTypeValuePercent("metro_data", incidents, failureTypes);
    }
    
    private void writeStateDataYtd(int year, int month) throws IOException {
        List<DBObject> incidents = getByState(incidentStateYtd, year, month);
        List<DBObject> failureTypes = getByState(equipmentFailureStateYtd, year, month);
        writeGenericTypeValuePercent("state_data", incidents, failureTypes);
    }
    
    private void writeNationalDataYtd(int year, int month) throws IOException {
        List<DBObject> incidents = getNational(incidentNationalYtd, year, month);
        List<DBObject> failureTypes = getNational(equipmentFailureNationalYtd, year, month);
        writeGenericTypeValuePercent("national_data", incidents, failureTypes);
    }
    
    public void aggregateCountyYtds(DBCollection sourceMonthlyCollection, DBCollection destinationYtdCollection) {
        AggregationOptions aggregationOptions = AggregationOptions.builder()
                .outputMode(AggregationOptions.OutputMode.CURSOR).build();
        
        DBObject query = new BasicDBObject();
        query.put("state", state);
        query.put("county", county);
        
        DBObject match = new BasicDBObject("$match", query);
        
        DBObject groupFields = new BasicDBObject();
        DBObject groupInner = new BasicDBObject("_id", groupFields);
        DBObject group = new BasicDBObject("$group", groupInner);
        groupFields.put("type", "$type");
        groupInner.put("count", new BasicDBObject("$sum", "$count"));
        
        DBObject projectFields = new BasicDBObject();
        DBObject project = new BasicDBObject("$project", projectFields);
        projectFields.put("_id", 0);
        projectFields.put("type", "$_id.type");
        projectFields.put("count", 1);
        
        List<DBObject> batchInserts = new ArrayList<DBObject>();
        
        for (int year = (reportYear-3); year < reportYear; year++) {
            query.put("year", year);
            
            for (int month = 1; month <= 12; month++) {
                
                query.put("month", new BasicDBObject("$lte", month));
                
                projectFields.put("state", new BasicDBObject("$literal", state));
                projectFields.put("county", new BasicDBObject("$literal", county));
                projectFields.put("year", new BasicDBObject("$literal", year));
                projectFields.put("month", new BasicDBObject("$literal", month));
                
                List<DBObject> pipeline = Arrays.asList(match, group, project);
                
                
                Cursor cursor = sourceMonthlyCollection.aggregate(pipeline, aggregationOptions);
                while (cursor.hasNext()) {
                    DBObject next = cursor.next();
                    batchInserts.add(next);
                }
            }
        }
        if (batchInserts.size() > 0) {
            destinationYtdCollection.insert(batchInserts);
        }
    }
    
    public void aggregateStateYtds(DBCollection sourceMonthlyCollection, DBCollection destinationYtdCollection) {
        
        AggregationOptions aggregationOptions = AggregationOptions.builder()
                .outputMode(AggregationOptions.OutputMode.CURSOR).build();
        
        DBObject query = new BasicDBObject();
        query.put("state", state);
        
        DBObject match = new BasicDBObject("$match", query);
        
        DBObject groupFields = new BasicDBObject();
        DBObject groupInner = new BasicDBObject("_id", groupFields);
        DBObject group = new BasicDBObject("$group", groupInner);
        groupFields.put("type", "$type");
        groupInner.put("count", new BasicDBObject("$sum", "$count"));
        
        DBObject projectFields = new BasicDBObject();
        DBObject project = new BasicDBObject("$project", projectFields);
        projectFields.put("_id", 0);
        projectFields.put("type", "$_id.type");
        projectFields.put("count", 1);
        
        List<DBObject> batchInserts = new ArrayList<DBObject>();
        
        for (int year = (reportYear-3); year < reportYear; year++) {
            query.put("year", year);
            
            for (int month = 1; month <= 12; month++) {
                
                query.put("month", new BasicDBObject("$lte", month));
                
                projectFields.put("state", new BasicDBObject("$literal", state));
                projectFields.put("year", new BasicDBObject("$literal", year));
                projectFields.put("month", new BasicDBObject("$literal", month));
                
                List<DBObject> pipeline = Arrays.asList(match, group, project);
                
                
                Cursor cursor = sourceMonthlyCollection.aggregate(pipeline, aggregationOptions);
                while (cursor.hasNext()) {
                    DBObject next = cursor.next();
                    batchInserts.add(next);
                }
            }
        }
        if (batchInserts.size() > 0) {
            destinationYtdCollection.insert(batchInserts);
        }
    }
    
    public void aggregateNationalYtds(DBCollection sourceMonthlyCollection, DBCollection destinationYtdCollection) {
        AggregationOptions aggregationOptions = AggregationOptions.builder()
                .outputMode(AggregationOptions.OutputMode.CURSOR).build();
        
        DBObject query = new BasicDBObject();
        
        DBObject match = new BasicDBObject("$match", query);
        
        DBObject groupFields = new BasicDBObject();
        DBObject groupInner = new BasicDBObject("_id", groupFields);
        DBObject group = new BasicDBObject("$group", groupInner);
        groupFields.put("type", "$type");
        groupInner.put("count", new BasicDBObject("$sum", "$count"));
        
        DBObject projectFields = new BasicDBObject();
        DBObject project = new BasicDBObject("$project", projectFields);
        projectFields.put("_id", 0);
        projectFields.put("type", "$_id.type");
        projectFields.put("count", 1);
        

        List<DBObject> batchInserts = new ArrayList<DBObject>();
        
        for (int year = (reportYear-3); year < reportYear; year++) {
            query.put("year", year);
            
            for (int month = 1; month <= 12; month++) {
                
                query.put("month", new BasicDBObject("$lte", month));
                
                projectFields.put("year", new BasicDBObject("$literal", year));
                projectFields.put("month", new BasicDBObject("$literal", month));
                
                List<DBObject> pipeline = Arrays.asList(match, group, project);
                
                
                Cursor cursor = sourceMonthlyCollection.aggregate(pipeline, aggregationOptions);
                while (cursor.hasNext()) {
                    DBObject next = cursor.next();
                    batchInserts.add(next);
                }
            }
        }
        if (batchInserts.size() > 0) {
            destinationYtdCollection.insert(batchInserts);
        }
    }
    
    private static Double getCount(DBObject next) {
        Object countObj = next.get("count");
        Double count = null;
        if (countObj instanceof Integer) {
            Integer countI = (Integer)countObj;
            count = countI.doubleValue();
        } else {
            count = (Double)next.get("count");
        }
        return count;
    }
    
    public void dropAggregateCollections() {
        incidentCountyYtd.drop();
        equipmentFailureCountyYtd.drop();
        
        incidentStateYtd.drop();
        equipmentFailureStateYtd.drop();
        
        incidentNationalYtd.drop();
        equipmentFailureNationalYtd.drop();
    }
    
    public void writeReport() throws IOException {
        
        aggregateCountyYtds(incidentCountyMonthly, incidentCountyYtd);
        aggregateCountyYtds(equipmentFailureCountyMonthly, equipmentFailureCountyYtd);
        
        aggregateStateYtds(incidentStateMonthly, incidentStateYtd);
        aggregateStateYtds(equipmentFailureStateMonthly, equipmentFailureStateYtd);
        
        aggregateNationalYtds(incidentNationalMonthly, incidentNationalYtd);
        aggregateNationalYtds(equipmentFailureNationalMonthly, equipmentFailureNationalYtd);
        
        writer.setIndent("    ");
        writer.beginObject(); // root
        writer.name("name").value(county);
        writer.name("state").value(state);
        
        DateTime currentDate = new DateTime();
        String currentMonth = currentDate.toString("MMMM");
        
        writer.name("current_month").value(currentMonth);
        writer.name("current_year").value(currentDate.getYear());
        writer.name("current_date").value(dateTimeFormatter.print(currentDate));
        
        writer.name("years").beginArray();
        
        for (int year = (reportYear-2); year <= reportYear; year++) {
            writer.beginObject();
            writer.name("year").value(year);
            
            writer.name("month_periods").beginArray();
            for (int month = 1; month <= 12; month++) {
                writer.beginObject(); // month
                writer.name("month").value(month);
                writeMetroData(year, month);
                writeStateData(year, month);
                writeNationalData(year, month);
                writer.endObject(); // month
            }
            writer.endArray(); // month_periods
            
            writer.name("YTD_periods").beginArray(); // YTD_periods
            for (int month = 1; month <= 12; month++) {
                writer.beginObject(); // month
                writer.name("month").value(month);
                writeMetroDataYtd(year, month);
                writeStateDataYtd(year, month);
                writeNationalDataYtd(year, month);
                writer.endObject(); // month
            }
            writer.endArray(); // YTD_periods
            
            writer.endObject(); // year
        }
        writer.endArray(); // years
        
        writer.endObject(); // root
        writer.close();
    }



    private static void usage() {
        System.err.println("usage: ReportWriter <year> <state> <county>");
        
    }
    
    public static void main(String[] args) throws IOException {
        // 2012 IL COOK
        
        
        ReportWriter writer = new ReportWriter(2013, "CA", "LOS ANGELES");
        writer.dropAggregateCollections();
        writer.writeReport();
        
        writer = new ReportWriter(2013, "MD", "HOWARD");
        writer.writeReport();
        
        writer = new ReportWriter(2013, "IL", "COOK");
        writer.writeReport();
        
        writer = new ReportWriter(2013, "KS", "");
        writer.writeReport();
    }



    

}
