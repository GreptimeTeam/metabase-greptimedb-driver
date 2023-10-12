# Metabase Databend Driver

## Installation

## Configuring

1. Once you've started up Metabase, open http://localhost:3000 , go to add a database and select "GreptimeDB".
2. You'll need to provide the Host/Port, Database Name, Username and Password.

### Prerequisites

- [Clojure](https://clojure.org/)

### Build from source

1. Clone metabase.

   ```shell
   git clone https://github.com/metabase/metabase
   ```

2. Clone metabase-greptime-driver repo

   ```shell
   git clone https://github.com/GreptimeTeam/metabase-greptimedb-driver
   ```

3. Prepare metabase dependencies

   ```shell
   cd metabase
   DRIVER_PATH=`readlink -f ../metabase-greptimedb-driver`
   clojure \
     -Sdeps "{:aliases {:greptimedb {:extra-deps {com.metabase/greptimedb-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
     -X:build:greptimedb \
     build-drivers.build-driver/build-driver! \
     "{:driver :greptimedb, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}"
   ```

4. Let's assume we download `metabase.jar` from the [Metabase jar](https://www.metabase.com/docs/latest/operations-guide/running-the-metabase-jar-file.html) to `~/metabase/` and we built the project above. Copy the built jar to the Metabase plugins directly and run Metabase from there!

   ```shell
   cd ~/metabase/
   java -jar metabase.jar
   ```

You should see a message on startup similar to:

```
2023-10-12 17:09:49,257 DEBUG plugins.lazy-loaded-driver :: Registering lazy loading driver :greptimedb...
2023-10-12 17:09:49,257 INFO driver.impl :: Registered driver :greptimedb (parents: [:sql-jdbc]) 🚚
```

### Known issues

1. Unsupport some SQL syntax, such as `SELECT * FROM table WHERE column BETWEEN (1, 2, 3)`
2. Semantic recognition of Column is inaccurate.
