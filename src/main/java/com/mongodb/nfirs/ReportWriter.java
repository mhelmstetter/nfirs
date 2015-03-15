package com.mongodb.nfirs;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.stream.JsonWriter;
import com.mongodb.BasicDBObject;
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
    
    JsonWriter writer = new JsonWriter(new OutputStreamWriter(System.out));
    
    public ReportWriter(int year, String state, String county) throws UnknownHostException {
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
        DBObject query = new BasicDBObject();
        query.put("state", state);
        query.put("county", county);
        
        DBObject orderBy = new BasicDBObject();
        orderBy.put("type", 1);
        orderBy.put("month", 1);
        
        // hack to make sure we reset the data while things are changing
        destinationYtdCollection.drop();
        
        for (int year = (reportYear-3); year < reportYear; year++) {
            query.put("year", year);
            //long existingCount = destinationYtdCollection.count(query);
            //logger.debug("Existing count for " + destinationYtdCollection + " for year " + year + " is " + existingCount);
            //if (existingCount == 0) {
                
                double yearToDate = 0;
                for (int month = 1; month <= 12; month++) {
                    
                    query.put("month", new BasicDBObject("$lte", month));
                    DBCursor cursor = sourceMonthlyCollection.find(query).sort(orderBy);
                    
                    logger.debug("db." + sourceMonthlyCollection + ".find(" + query + ")");
                    
                    String lastType = null;
                    while (cursor.hasNext()) {
                        DBObject next = cursor.next();
                        
                        String type = (String)next.get("type");
                        Double count = getCount(next);
                        
                        if (type.equals(lastType)) {
                            yearToDate += count;
                        } else {
                            DBObject ytd = new BasicDBObject();
                            ytd.put("state", state);
                            ytd.put("county", county);
                            ytd.put("year", year);
                            ytd.put("month", month);
                            ytd.put("count", yearToDate);
                            ytd.put("type", lastType);
                            destinationYtdCollection.insert(ytd);
                            
                            yearToDate = count; 
                        }
                        lastType = type;
                    }
                }
            //}
        }
    }
    
    public void aggregateStateYtds(DBCollection sourceMonthlyCollection, DBCollection destinationYtdCollection) {
        DBObject query = new BasicDBObject();
        query.put("state", state);
        
        DBObject orderBy = new BasicDBObject();
        orderBy.put("type", 1);
        orderBy.put("month", 1);
        
        // hack to make sure we reset the data while things are changing
        destinationYtdCollection.drop();
        
        for (int year = (reportYear-3); year < reportYear; year++) {
            query.put("year", year);
            long existingCount = destinationYtdCollection.count(query);
            logger.debug("Existing count for " + destinationYtdCollection + " for year " + year + " is " + existingCount);
            //if (existingCount == 0) {
                
                double yearToDate = 0;
                for (int month = 1; month <= 12; month++) {
                    
                    query.put("month", new BasicDBObject("$lte", month));
                    DBCursor cursor = sourceMonthlyCollection.find(query).sort(orderBy);
                    
                    logger.debug("db." + sourceMonthlyCollection + ".find(" + query + ")");
                    
                    String lastType = null;
                    while (cursor.hasNext()) {
                        DBObject next = cursor.next();
                        
                        String type = (String)next.get("type");
                        Double count = getCount(next);
                        
                        if (type.equals(lastType)) {
                            yearToDate += count;
                        } else {
                            DBObject ytd = new BasicDBObject();
                            ytd.put("state", state);
                            ytd.put("year", year);
                            ytd.put("month", month);
                            ytd.put("count", yearToDate);
                            ytd.put("type", lastType);
                            destinationYtdCollection.insert(ytd);
                            
                            yearToDate = count; 
                        }
                        lastType = type;
                    }
                }
            //}
        }
    }
    
    public void aggregateNationalYtds(DBCollection sourceMonthlyCollection, DBCollection destinationYtdCollection) {
        DBObject query = new BasicDBObject();
        
        DBObject orderBy = new BasicDBObject();
        orderBy.put("type", 1);
        orderBy.put("month", 1);
        
        // hack to make sure we reset the data while things are changing
        destinationYtdCollection.drop();
        
        for (int year = (reportYear-3); year < reportYear; year++) {
            query.put("year", year);
            long existingCount = destinationYtdCollection.count(query);
            logger.debug("Existing count for " + destinationYtdCollection + " for year " + year + " is " + existingCount);
            //if (existingCount == 0) {
                
                double yearToDate = 0;
                for (int month = 1; month <= 12; month++) {
                    
                    query.put("month", new BasicDBObject("$lte", month));
                    DBCursor cursor = sourceMonthlyCollection.find(query).sort(orderBy);
                    
                    logger.debug("db." + sourceMonthlyCollection + ".find(" + query + ")");
                    
                    String lastType = null;
                    while (cursor.hasNext()) {
                        DBObject next = cursor.next();
                        
                        String type = (String)next.get("type");
                        Double count = getCount(next);
                        
                        if (type.equals(lastType)) {
                            yearToDate += count;
                        } else {
                            DBObject ytd = new BasicDBObject();
                            ytd.put("year", year);
                            ytd.put("month", month);
                            ytd.put("count", yearToDate);
                            ytd.put("type", lastType);
                            destinationYtdCollection.insert(ytd);
                            
                            yearToDate = count; 
                        }
                        lastType = type;
                    }
                }
            //}
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
        
        for (int year = (reportYear-3); year < reportYear; year++) {
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
        if (args.length < 3) {
            usage();
            System.exit(-1);
        }
        int year = Integer.parseInt(args[0]);
        
        
        ReportWriter writer = new ReportWriter(year, args[1], args[2]);
        
        
        writer.writeReport();
    }



    

}
