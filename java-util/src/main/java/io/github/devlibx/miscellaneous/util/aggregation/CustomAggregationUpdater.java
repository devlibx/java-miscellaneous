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
     * A custom counter which just decrements the key by 1 or skip if the value is below threshold
     */
    class DecrementCounter implements TimeWindowDataAggregationHelper.IAggregationUpdater<StringObjectMap> {
        private final boolean skip;
        private final long threshold;

        public DecrementCounter(boolean skip, long threshold) {
            this.skip = skip;
            this.threshold = threshold;
        }

        @Override
        public void update(StringObjectMap data, String key, StringObjectMap event) {
            long existingValue = data.getLong(key) == null
                    ? 0L
                    : data.getLong(key);
            if (!skip || existingValue > threshold) {
                data.put(key, existingValue - 1);
            }
        }
    }

    /**
     * A custom counter which just add long values
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
     * A custom counter which just add double values
     */
    class AddDoubleValue implements TimeWindowDataAggregationHelper.IAggregationUpdater<StringObjectMap> {
        private final double value;

        public AddDoubleValue(double value) {
            this.value = value;
        }

        @Override
        public void update(StringObjectMap data, String key, StringObjectMap event) {
            double existingValue = data.getDouble(key) == null
                    ? 0.0D
                    : data.getDouble(key);
            data.put(key, existingValue + value);
        }
    }

    /**
     * A custom counter which just appends the string to the existing set
     */
    class StringAppender implements TimeWindowDataAggregationHelper.IAggregationUpdater<StringObjectMap> {
        private final String newString;
        private final String separator;
        private final int maxLength;

        public StringAppender(String newString, String separator, int maxLength) {
            this.newString = newString;
            this.separator = separator;
            this.maxLength = maxLength;
        }

        @Override
        public void update(StringObjectMap data, String key, StringObjectMap event) {
            String existingValue = data.getString(key) == null
                    ? ""
                    : data.getString(key);
            Set<String> merchants = new HashSet<>();
            for (String s : existingValue.split(this.separator)) {
                if (!Strings.isNullOrEmpty(s)) {
                    merchants.add(s);
                }
            }
            if(merchants.size() > maxLength) {
                data.put(key, String.join(this.separator, merchants));
            }
            else {
                merchants.add(newString);
                data.put(key, String.join(this.separator, merchants));
            }
        }
    }
}
