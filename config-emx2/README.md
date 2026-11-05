# EMX2 configuration for paper extracting via the HPC cluster

For paper extracting jobs to be run via EMX2, the following configuration needs to be in place. The other bridge settings (`POLL_INTERVAL`, `HPC_*`, ...) are described in the [main README](../README.md#configuration).

- SERVER: There needs to be a server (the EMX2 server) that users submit their jobs to. The (publicly available) URL of this server is passed to the emx2-hpc-bridge via the `EMX2_SERVER` environment variable, for example `https://my-server.org`. The server needs to be reachable from the HPC cluster that does the polling. Only the cluster connects to the server; the server never needs to reach the cluster.
- BRIDGE SCHEMA: The bridge needs a separate schema on the server, used only for the emx2-hpc-bridge job submissions. Do not reuse a schema that holds other data. The bridge schema must be private by default: no users have a role on it (no Viewer, Editor, ... members) and `anonymous` has no access. Access is only granted through the custom roles described below. The schema may have any name; the name is passed to the emx2-hpc-bridge via the `EMX2_SCHEMA` environment variable.
- TABLES: The bridge schema should contain two tables, `Jobs` and `Status` (table names are currently not configurable). The configuration of these tables is in [emx2_hpc_bridge_config.csv](emx2_hpc_bridge_config.csv).
- STATUS VALUES: The `Status` table must contain the status labels the bridge uses. Import [Status.csv](Status.csv) into the bridge schema; without these rows, creating jobs and updating their status fails.
- POLLER ACCOUNT: Create an account for the bridge on the HPC side. In the bridge schema, create the `Poller` role using the [poller role template](#poller-role-template) and add the account to the bridge schema with that role, see the [member template](#member-template). The role lets the poller READ and UPDATE all rows of the `Jobs` table (including downloading the input files) and READ the `Status` table. After creating the account, generate a token for it and set it on the HPC side via the `EMX2_POLLER_JWT_TOKEN` environment variable.
- USER CONFIG: Because the bridge schema is private, users have no access to the `Jobs` or `Status` tables until they are given a role. For **each user** that needs to submit jobs:
  - Create a user account
  - Create a custom role in the bridge schema using the [user role template](#user-role-template) (replace `[USERNAME]` with the actual username)
  - Add the user to the bridge schema and assign the created role, see the [member template](#member-template)
  - As a result of this setup, a user can only create and view their own jobs

Run the templates below against the bridge schema's GraphQL endpoint (`<EMX2_SERVER>/<EMX2_SCHEMA>/graphql`) as a Manager or Owner of the bridge schema.

Note that a user can fill in all columns when inserting a job, including `status` and `output`. The bridge only picks up jobs with status `CREATED`, so this cannot disturb the cluster, but a job's `output` is only guaranteed to come from the cluster for jobs the bridge processed.

### Poller role template

```graphql
mutation {
  change(
    roles: [
      {
        name: "Poller"
        permissions: [
          {
            table: "Jobs"
            select: true
            insert: false
            update: true
            delete: false
            isRowLevel: false
          }
          {
            table: "Status"
            select: true
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

Add the poller account to the bridge schema with this role, see the [member template](#member-template) (use the poller account and `Poller` as role).

### User role template

```graphql
mutation {
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
            select: true
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

Users can read `Status` so the job form can show the status options and their jobs' status can be displayed.

### Member template

```graphql
mutation {
  change(members: [{ email: "[USERNAME]", role: "Submit_[USERNAME]" }]) {
    message
  }
}
```
