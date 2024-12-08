# rslite

Sqlite row based synchronization for local dbs.
Ideal for **incremental restore/backups** and **off-network transportation**.

At the moment issues an insert or replace query on the destination db for each row, so it may be slow for large datasets and large row values.

### installation
- from source: ```console go install github.com/alvarolm/rslite@latest```

### Usage:
```console
Usage:
  syncs [source db] [target db] [flags]

Examples:

  # Sync all tables from source to target
  rslite source.db target.db

  # Sync only specific tables
  rslite source.db target.db -t users,orders

  # Sync using "Primary Key" filters (sync records with "Primary Key" > 100)
  rslite source.db target.db -f gt -p 100

  # Sync specific tables without deleting existing records
  rslite source.db target.db -t users,orders -n

  # Complex sync with filters and specific tables
  rslite source.db target.db -t users,orders -f gte -p 1000 -n

Flags:
  -f, --filter string    filter type: gt, lt, gte, or lte
  -h, --help             help for syncs
  -n, --nodelete         don't delete records from target
  -t, --tables strings   tables to sync (comma-separated)
  -v, --value string     filter value
```

#### TODO:
- implement content hashing comparison
- more testing

MIT License
Copyright (c) 2024 Alvaro Leiva Miranda (alvaro@remote-workbench.com)
