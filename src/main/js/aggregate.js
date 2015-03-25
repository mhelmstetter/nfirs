db.BasicIncident.aggregate([
   {$group: {_id:{
       state:"$CopyBasicIncident_FireDeptState", 
       county:"$Incident_AddressHowardLACook_WB_CookLAHowardCountyZipCodes_County", 
       year:"$CopyBasicIncident_Year", 
       month:"$CopyBasicIncident_Month", 
	   type:"$CopyBasicIncident_Calc_IncidentTypesChuckGroup"},
 count: { $sum: 1 }}},
   {$project: {_id:0, state:"$_id.state", county:"$_id.county", year:"$_id.year", month:"$_id.month", type:"$_id.type", count:1}},
   {$out : "IncidentCountyMonthly"}
 ])
 
db.BasicIncident.aggregate([
   {$group: {_id:{
       state:"$CopyBasicIncident_FireDeptState", 
       year:"$CopyBasicIncident_Year", 
       month:"$CopyBasicIncident_Month", 
	   type:"$CopyBasicIncident_Calc_IncidentTypesChuckGroup"},
 count: { $sum: 1 }}},
   {$project: {_id:0, state:"$_id.state", year:"$_id.year", month:"$_id.month", type:"$_id.type", count:1}},
   {$out : "IncidentStateMonthly"}
 ])
 
db.IncidentStateMonthly.aggregate([
   {$group: {_id:{ 
       year:"$year", 
       month:"$month", 
	   type:"$type"},
 count: { $sum: "$count" }}},
   {$project: {_id:0, year:"$_id.year", month:"$_id.month", type:"$_id.type", count:1}},
   {$out : "IncidentNationalMonthly"}
 ])
 
db.BasicIncident.aggregate([
   {$group: {_id:{ 
       year:"$CopyBasicIncident_Year", 
       month:"$CopyBasicIncident_Month", 
	   type:"$CopyBasicIncident_Calc_IncidentTypesChuckGroup"},
 count: { $sum: 1 }}},
   {$project: {_id:0, year:"$_id.year", month:"$_id.month", type:"$_id.type", count:1}},
   {$out : "IncidentNationalMonthly"}
 ])
 
db.EquipFailbyStateYear_FireDeptStateMoYearItem.aggregate([
   {$group: {_id:{ 
       year:"$year", 
       month:"$month", 
	   type:"$type"},
 count: { $sum: 1 }}},
   {$project: {_id:0, year:"$_id.year", month:"$_id.month", type:"$_id.type", count:1}},
   {$out : "EquipmentFailureNationalMonthly"}
 ])