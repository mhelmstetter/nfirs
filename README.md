# nfirs

## Development Environment Setup

1. Import a subset of the `BasicIncident` data:
    
    ```
    mongoimport --type tsv BasicIncidentHowardLACook_00000 --headerline -d nfirs -c BasicIncident
    
    ```
    
1. Data hack, change COOK, IL to COOK, MD so that we have more than one county for MD. Otherwise county and state data will have the same numbers
since we have an incomplete dataset.

    ```
    db.BasicIncident.update({"Incident_AddressHowardLACook_WB_CookLAHowardCountyZipCodes_County":"COOK", "CopyBasicIncident_FireDeptState":"IL"}, {$set:{"CopyBasicIncident_FireDeptState":"MD"}}, {multi:true})
    ```
    
1. Import summary collections from csv/tsv files:

    ```
    mongoimport --type csv EquipFailbyStateYear_FireDeptStateMoYearItem.csv --headerline -d nfirs -c EquipFailbyStateYear_FireDeptStateMoYearItem --drop
    mongoimport --type csv EquipFailure_CookLAHoward_FireEquipItembyCtyMoYR.csv --headerline -d nfirs -c EquipFailure_CookLAHoward_FireEquipItembyCtyMoYR --drop
    mongoimport --type tsv FireEquipItembyCtyYR_00000 --headerline -d nfirs -c FireEquipItembyCtyYR
    
    mongoimport --type csv MoYrStateTypeCount --headerline -d nfirs -c IncidentStateMonthly --drop
    mongoimport --type csv YrCntyTypeIncNum --headerline -d nfirs -c IncidentCountyMonthly --drop
    mongoimport --type csv IncidentCountyMonthly.csv --headerline -d nfirs -c IncidentCountyMonthly --drop
    
    mongoimport --type csv HowardLACookBasicIncident_VariousIncCounts_MoYrCntyTypeIncNum.csv --headerline -d nfirs -c IncidentCountyMonthly --drop
    
    ```
    
1. Data hack, does not include the state, only county name which breaks our assumptions of consistency and being able to generically process various
summary collections. Note also that we manually renamed columns to be consistent by editing the header rows in the csv files before importing them into
MongoDB.

    ```
    use nfirs
    db.EquipFailure_CookLAHoward_FireEquipItembyCtyMoYR.update({"county" : "HOWARD"}, {$set:{"state":"MD"}}, {multi:true})
    db.EquipFailure_CookLAHoward_FireEquipItembyCtyMoYR.update({"county" : "COOK"}, {$set:{"state":"IL"}}, {multi:true})
    db.EquipFailure_CookLAHoward_FireEquipItembyCtyMoYR.update({"county" : "LOS ANGELES"}, {$set:{"state":"CA"}}, {multi:true})
    
    db.IncidentCountyMonthly.update({"county" : "COOK"}, {$set:{"state":"IL"}}, {multi:true})
    db.IncidentCountyMonthly.update({"county" : "HOWARD"}, {$set:{"state":"MD"}}, {multi:true})
    db.IncidentCountyMonthly.update({"county" : "LOS ANGELES"}, {$set:{"state":"CA"}}, {multi:true})
    ```
    
1. Run aggregations using mongo shell:

    ```
    mongo localhost:27017/nfirs aggregate.js
    ```



