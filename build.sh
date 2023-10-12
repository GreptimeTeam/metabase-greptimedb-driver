DRIVER_PATH=`readlink -f ~/greptime/metabase-greptimedb-driver`
clojure \
  -Sdeps "{:aliases {:greptimedb {:extra-deps {com.metabase/greptimedb-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
  -X:build:greptimedb \
  build-drivers.build-driver/build-driver! \
  "{:driver :greptimedb, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}" && \
  cp ../metabase-greptimedb-driver/target/greptimedb.metabase-driver.jar plugins && clojure -M:run