# IDRA Customer Data => Nerva

This project is to plug in IDRA customer data with Nerva.

## Setup keys to access IDRA Commercial DB
Setup the following text file:

```
HOST= database host URL
PORT= port
USERNAME= database username
PASSWORD= database password
DATABASE= main database/schema
```

Save the above in a text file `.env.prod.txt` in the `app` folder main directory. This file will be used in the ingestion script that moves the data between databases.

## Connecting to Nerva

Go to [Nerva](nerva.oreyeon.ai).

Website Credentials:
- Username: XXXX
- Password: XXXX

Nerva User Credentials:
- Username: XXXX
- Password: XXXX


## Offline Fields into Online Fields

| Offline         | Online          |
| -------         | ------          |
| `modelType`     | `source`        |
| `type`          | `finalSource`   |
| `modelColor`    | `color`         |
| `color`         | `finalColor`    |
| `modelSize`     | `size`          |
| `size`          | `finalSize`     |
| `modelMaterial` | `material`      |
| `material`      | `finalMaterial` |
| `comment`       | `Comment`       |


Additional steps to do beside the above mapping:
- `createdTime`: Change format of datetime to remove T and Z.
- `finalSize`: Reformatted to remove metric (inch) from string, and convert to float.

## Image Storage

The images extracted from the offline database will be setup in:
- Bucket: `idra-commercial`
- Prefix: `FOD Images`

## Data Points
The images are available in `idra-commercial/FOD Images`.

The mapping of the `reportID` <-- `runID` and the mapping of the `fodID` <-- `id` can be found in `idra-commercial/FOD-Images-Map/` in the following formats respectively: `report-offline-RUNID-online-REPORTID.json` and `fods-offline-RUNID-online-REPORTID.json`.

The data is also available in `tblReport` and `tblFod` in the commercial IDRA db.