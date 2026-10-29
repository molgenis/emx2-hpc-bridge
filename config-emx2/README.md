# EMX2 configuration for Paper extracting via HPC cluster

For the papar extracting jobs to be run via emx2, the folling configuration needs to be in place

- SERVER: There needs to be a server ( the emx2 server) that is used for a user to submit the jobs to. The (publicly available) name of this server needs to be set as part of the emx2-hpc-brige python script config and is passed via a enviroment varaible `EMX2_SERVER`. For example https://my-server.org. The server needs to reachable by the HPC cluster that is doing the polling.
- SCHEMA: On the sever there needs to be a schema that contains a table to hold the job submisions. This schema may have any name, the name used needs to be set on the emx2-hpc-brige python script via the `EMX2_SCHEMA` enviroment variable. 
- TABLES: The schema on the server should contain 2 tables, `Jobs` and `Status` ( table naming is currently not configurable). The exact configuration of these tables show in [emx2_hpc_brige_config.csv](emx2_hpc_brige_config.csv). 
- POLLER ACCOUNT: An account needs to be created for the script-user on the HPC side to be able to READ and WRITE to the Jobs table, and READ only for the Status table. After creating the account generate a token for this account, this token needs to be set on the HPC side via the `EMX2_POLLER_JWT_TOKEN` enviroment variable.
- USER CONFIG: no users should me allow to READ/WRITE/UPDATE/DELETE the `Jobs` or `Status` tables. For **each user** that need to sumbit jobs:
  - Create a user account
  - Create a custom role on the schema via the gql api ( replace [USERNAME]) with the actual username, for example use the template below
  - Add the user to the schema and assign the created role
  - As the result of this setup, user can only create and view there own jobs
### permission query template  
``` mutation {
  change(
    roles: [
      {
        name: "Submit_[USERNAME]"
        permissions: [
          {
            table: "Jobs"
            select: true
            insert: true
            update: false
            delete: false
            isRowLevel: true
          }
          {
            table: "Status"
            select: false
            insert: false
            update: false
            delete: false
            isRowLevel: false
          }
        ]
      }
    ]
  ) {
    message
  }
}
```
