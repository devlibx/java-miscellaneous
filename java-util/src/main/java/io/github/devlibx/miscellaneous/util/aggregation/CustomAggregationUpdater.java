package io.github.devlibx.miscellaneous.util.aggregation;

import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;

import java.util.HashSet;
import java.util.Set;

public interface CustomAggregationUpdater {

    /**
     * A custom counter which just increments the key by 1
     */
    class IncrementCounter implements TimeWindowDataAggregationHelper.IAggregationUpdater<StringObjectMap> {
        @Override
        public void update(StringObjectMap data, String key, StringObjectMap event) {
            long existingValue = data.getLong(key) == null
                    ? 0L
                    : data.getLong(key);
            data.put(key, existingValue + 1);
        }
    }

    /**
     * A custom counter which just add value
     */
    class AddValue implements TimeWindowDataAggregationHelper.IAggregationUpdater<StringObjectMap> {
        private final long value;

        public AddValue(long value) {
            this.value = value;
        }

        @Override
        public void update(StringObjectMap data, String key, StringObjectMap event) {
            long existingValue = data.getLong(key) == null
                    ? 0L
                    : data.getLong(key);
            data.put(key, existingValue + value);
        }
    }

    /**
     * A custom counter which just appends the string to the existing set
     */
    class StringAppender implements TimeWindowDataAggregationHelper.IAggregationUpdater<StringObjectMap> {
        private final String newString;

        public StringAppender(String newString) {
            this.newString = newString;
        }

        @Override
        public void update(StringObjectMap data, String key, StringObjectMap event) {
            String existingValue = data.getString(key) == null
                    ? ""
                    : data.getString(key);
            Set<String> merchants = new HashSet<>();
            for (String s : existingValue.split(",")) {
                if (!Strings.isNullOrEmpty(s)) {
                    merchants.add(s);
                }
            }
            merchants.add(newString);
            data.put(key, String.join(",", merchants));
        }
    }
}
