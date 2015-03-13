# nfirs

## Development Environment Setup

1. Import a subset of the `BasicIncident` data:
    
    ```
    mongoimport --type tsv BasicIncidentHowardLACook_00000 --headerline -d nfirs -c BasicIncident
    ```
    
1. Run aggregations using mongo shell:

    ```
    mongo localhost:27017/nfirs aggregate.js
    ```



