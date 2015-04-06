
db.IncidentCountyMonthly.aggregate([
   {$group: {_id:{ 
       year:"$year",
	   county:"$county"},
 count: { $sum: "$count" }}},
   {$project: {_id:0, year:"$_id.year", county:"$_id.county", count:1}},
   {$out : "Check_IncidentCountyYear"}
 ])