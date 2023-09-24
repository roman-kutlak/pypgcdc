# pypgcdc

Change Data Capture (CDC) tool for Postgres

This project is a Python implementation of a Postgres CDC client.
It is intended to be used as a library for building CDC applications.
It is not intended to be used as a standalone application but there is a running example useful as a starting point.

The main problem with CDC and Postgres in practice is that Postgres runs with a hot stand-by used for failover
in case of a primary node failure. The change is usually behind a DNS record so clients just re-connect and run fine.
The problem is that the replication slot is not copied over to the new primary node and the CDC client will either
fail to start (no slot) or create a slot but potentially miss some data (slot created after some data was written).

You can, of course, create a new slot, do the initial sync where you copy over all the available data, 
and then start replicating as usual. The problem is that the initial sync can take a long time if you have big tables.

This library doesn't really solve the problem, but it provides a way to react to the failover event. The idea is
that you add triggers to the tables you want to replicate (_published tables_) and store the inserts/updates/deletes
in separate tables (_log tables_).
The initial sync when you start your CDC app can just `select *` from the published tables and then tail the changes
on these tables. You will need some persistence to store the commits you have already processed so when a failover
event occurs, you can use the last processed commit to select any changes from the log tables and then carry on
tailing the published tables. This way you can avoid the initial sync and catch up with any pending changes faster.


## Env Vars

* PYPGCDC_DSN, default "postgres://postgres:postgrespw@localhost:5432/test" -- Postgres connection string
* PYPGCDC_SLOT, default "test_slot" -- Postgres replication slot name
* PYPGCDC_PUBLICATION, default "test_publication" -- Postgres publication name
* PYPGCDC_LSN, default 0 -- Postgres LSN to start from
* PYPGCDC_VERBOSE, default "False" -- A flag used to control print output of the example datastore. 
  Use one of ("1", "true", "yes") to enable more verbose output.


## Example

The library comes with an example which can be used to see how it works. The example requires a running 
Postgres database with some tables and an existing publication. The example will create a replication slot
if it doesn't exist and start tailing the changes. The example will print the changes to stdout.

Once you finish with the example, remember to drop the replication slot. Leaving an unused replication slot
is dangerous as the WAL files used for replication might not be removed, and you can run out of disk space
(not an issue on your local computer but quite a problem on your production servers...).
