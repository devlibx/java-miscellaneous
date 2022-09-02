CONFIG=$PWD/flink/src/main/resources/dummy_do_not_use.properties
flink run \
  -c io.github.devlibx.miscellaneous.flink.job.missedevent.Main \
  -p 1 \
 flink/target/flink-0.0.9-SNAPSHOT.jar \
 --config $CONFIG
