package io.github.devlibx.miscellaneous.flinkfat.common;

import com.google.common.base.Objects;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.MainTemplateV2;
import io.github.devlibx.easy.flink.utils.v2.config.SourceConfig;
import io.github.devlibx.miscellaneous.flink.common.StringObjectMapSerializer;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

public class TestFlinkJob implements MainTemplateV2.RunJob<io.github.devlibx.easy.flink.utils.v2.config.Configuration> {

    public static void main(String[] args) throws Exception {
        String jobName = "MissingEventHandlerJob";
        for (int i = 0; i < args.length; i++) {
            if (Objects.equal(args[i], "--name")) {
                jobName = args[i + 1];
                break;
            }
        }
        TestFlinkJob job = new TestFlinkJob();
        MainTemplateV2 template = new MainTemplateV2();
        template.main(args, jobName, job, io.github.devlibx.easy.flink.utils.v2.config.Configuration.class);
    }

    @Override
    public void run(StreamExecutionEnvironment env, io.github.devlibx.easy.flink.utils.v2.config.Configuration configuration, Class<io.github.devlibx.easy.flink.utils.v2.config.Configuration> aClass) {

        env.registerTypeWithKryoSerializer(StringObjectMap.class, StringObjectMapSerializer.class);

        String name = configuration.getEnvironment().getJobName();
        int stopAfterSec = configuration.getMiscellaneousProperties().getInt("stopAfterSec", 20);

        SourceConfig sourceConfig = configuration.getSourceByName("mainInput")
                .orElseThrow(() -> new RuntimeException("Did not find source with name=mainInput in config file"));

        sourceConfig.getKafkaSourceWithStringObjectMap(env)
                .keyBy(new KeySelectorExt())
                .process(new CustomProcessor())
                .addSink(new SlowSink(stopAfterSec));
    }

    public static class SlowSink extends RichSinkFunction<StringObjectMap> {
        private final int finalCount;

        public SlowSink(int finalCount) {
            this.finalCount = finalCount;
        }

        @Override
        public void invoke(StringObjectMap value, Context context) throws Exception {
            System.out.println("--> Value=" + value + " End=" + finalCount);
        }
    }

    static class CustomProcessor extends KeyedProcessFunction<String, StringObjectMap, StringObjectMap> {
        private transient MapState<String, String> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>(
                    "map",
                    String.class,
                    String.class
            );
            mapStateDescriptor.enableTimeToLive(ttlConfig);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(StringObjectMap value, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx, Collector<StringObjectMap> out) throws Exception {
            mapState.put(JsonUtils.asJson(value), JsonUtils.asJson(value));
            out.collect(value);
        }
    }

    static class KeySelectorExt implements KeySelector<StringObjectMap, String> {
        @Override
        public String getKey(StringObjectMap value) throws Exception {
            return JsonUtils.asJson(value);
        }
    }
}
