# nfirs

## Development Environment Setup

1. Import a subset of the `BasicIncident` data:
    
    ```
    mongoimport --type tsv BasicIncidentHowardLACook_00000 --headerline -d nfirs -c BasicIncident
    ```

