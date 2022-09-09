package io.github.devlibx.miscellaneous.flink.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.experimental.Delegate;

public class ExistingState {
    @Delegate
    private  StringObjectMap stringObjectMap = new StringObjectMap();

    public static ExistingState from(StringObjectMap in) {
        ExistingState es = new ExistingState();
        es.stringObjectMap = in;
        return es;
    }
}
