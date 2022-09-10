package io.github.devlibx.miscellaneous.flink.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import lombok.experimental.Delegate;

public class ExistingState {
    @Delegate
    private StringObjectMap stringObjectMap = new StringObjectMap();

    public static ExistingState from(StringObjectMap in) {
        ExistingState es = new ExistingState();
        es.stringObjectMap = in;
        return es;
    }

    /**
     * Specific get method which works with KayPair
     */
    public <T> T get(KeyPair keyPair, Class<T> cls) {
        return get(keyPair.compiledStringKey(), cls);
    }

    /**
     * Specific get method which works with KayPair
     */
    public <T> T get(KeyPair keyPair, String subKey, Class<T> cls) {
        return get(keyPair.compiledStringKey(), subKey, cls);
    }

    /**
     * Custom put with Key Pair as key
     */
    public static ExistingState from(KeyPair keyPair, StringObjectMap value) {
        StringObjectMap s = StringObjectMap.of(keyPair.compiledStringKey(), value);
        return ExistingState.from(s);
    }

    /**
     * Custom put with Key Pair as key
     */
    public static ExistingState from(KeyPair keyPair1, StringObjectMap value1, KeyPair keyPair2, StringObjectMap value2) {
        StringObjectMap s = StringObjectMap.of(keyPair1.compiledStringKey(), value1, keyPair2.compiledStringKey(), value2);
        return ExistingState.from(s);
    }

    /**
     * Custom put with Key Pair as key
     */
    public static ExistingState from(KeyPair keyPair1, StringObjectMap value1, KeyPair keyPair2, StringObjectMap value2, KeyPair keyPair3, StringObjectMap value3) {
        StringObjectMap s = StringObjectMap.of(keyPair1.compiledStringKey(), value1, keyPair2.compiledStringKey(), value2, keyPair3.compiledStringKey(), value3);
        return ExistingState.from(s);
    }
}
