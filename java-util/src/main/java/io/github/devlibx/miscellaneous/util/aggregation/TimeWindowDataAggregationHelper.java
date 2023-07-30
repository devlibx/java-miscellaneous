package io.github.devlibx.miscellaneous.util.aggregation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimeWindowDataAggregationHelper<T> {
    private final int daysCount;
    private final int daysHoursCount;
    private final int hoursCount;
    private final int minutesCount;

    public TimeWindowDataAggregationHelper(Config config) {
        this.hoursCount = config.hourAggregationWindow;
        this.minutesCount = config.minuteAggregationWindow;
        if (config.getDayHourAggregationWindow() > 0) {
            this.daysCount = 0;
            this.daysHoursCount = config.dayHourAggregationWindow;
        } else {
            this.daysCount = config.dayAggregationWindow;
            this.daysHoursCount = 0;
        }
    }

    public void process(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        processDayHour(aggregation, currentTime, event, eventTime, updater);
        processDay(aggregation, currentTime, event, eventTime, updater);
        processHours(aggregation, currentTime, event, eventTime, updater);
        processMinutes(aggregation, currentTime, event, eventTime, updater);
    }

    void processDayHour(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (daysHoursCount <= 0) return;

        int keysCount =  getDaysSinceLastUpdate(currentTime, aggregation);

        if(keysCount > daysHoursCount) {
            aggregation.getDaysHours().clear();
            keysCount = 0;
        }

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getDayHourKeys(currentTime, keysCount);
        List<String> keysInInput = getDayHourKeys(currentTime, daysHoursCount);
        Set<String> aggKeys = new HashSet<>(aggregation.getDaysHours().keySet());

        keysToRemove.forEach(keysInInput::remove);

        keysInInput.forEach(aggKeys::remove);

        aggKeys.forEach(keyToRemove -> updater.expired(aggregation.getDaysHours(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getMonthOfYear() + "-" + eventTime.getDayOfMonth();
        if (eventTime.isAfter(currentTime.minusDays(daysHoursCount)) && !eventTime.isAfter(currentTime)) {
            if (!aggregation.getDaysHours().containsKey(key)) {
                aggregation.getDaysHours().put(key, new StringObjectMap());
            }
            StringObjectMap dayData = aggregation.getDaysHours().getStringObjectMap(key);
            updater.update(dayData, eventTime.getHourOfDay() + "", event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    void processDay(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (daysCount <= 0) return;

        int keysCount = getDaysSinceLastUpdate(currentTime, aggregation);

        if(keysCount > daysCount) {
            aggregation.getDays().clear();
            keysCount = 0;
        }

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getDayKeys(currentTime, keysCount);
        List<String> keysInInput = getDayKeys(currentTime, daysCount);
        Set<String> aggKeys = new HashSet<>(aggregation.getDays().keySet());

        keysToRemove.forEach(keysInInput::remove);

        keysInInput.forEach(aggKeys::remove);

        aggKeys.forEach(keyToRemove -> updater.expired(aggregation.getDays(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getMonthOfYear() + "-" + eventTime.getDayOfMonth();
        if (eventTime.isAfter(currentTime.minusDays(daysCount)) && !eventTime.isAfter(currentTime)) {
            updater.update(aggregation.getDays(), key, event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    void processHours(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (hoursCount <= 0) return;

        int keysCount = getHoursSinceLastUpdate(currentTime, aggregation);

        if(keysCount > hoursCount) {
            aggregation.getHours().clear();
            keysCount = 0;
        }

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getHoursKeys(currentTime, keysCount);
        List<String> keysInInput = getHoursKeys(currentTime, hoursCount);
        Set<String> aggKeys = new HashSet<>(aggregation.getHours().keySet());

        keysToRemove.forEach(keysInInput::remove);

        keysInInput.forEach(aggKeys::remove);

        aggKeys.forEach(keyToRemove -> updater.expired(aggregation.getHours(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getDayOfMonth() + "-" + eventTime.getHourOfDay();
        if (eventTime.isAfter(currentTime.minusHours(hoursCount)) && !eventTime.isAfter(currentTime)) {
            updater.update(aggregation.getHours(), key, event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    void processMinutes(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (minutesCount <= 0) return;

        int keysCount = getMinutesSinceLastUpdate(currentTime, aggregation);

        if(keysCount > minutesCount) {
            aggregation.getMinutes().clear();
            keysCount = 0;
        }

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getMinuteKeys(currentTime, keysCount);
        List<String> keysInInput = getMinuteKeys(currentTime, minutesCount);
        Set<String> aggKeys = new HashSet<>(aggregation.getMinutes().keySet());

        keysToRemove.forEach(keysInInput::remove);

        keysInInput.forEach(aggKeys::remove);

        aggKeys.forEach(keyToRemove -> updater.expired(aggregation.getMinutes(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getHourOfDay() + "-" + eventTime.getMinuteOfHour();
        if (eventTime.isAfter(currentTime.minusMinutes(minutesCount)) && !eventTime.isAfter(currentTime)) {
            updater.update(aggregation.getMinutes(), key, event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    List<String> getDayKeys(DateTime time) {
        return getDayKeys(time, daysCount);
    }

    List<String> getDayKeys(DateTime time, int keysCount) {
        DateTime start = time.minusDays(keysCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusDays(1);
            String dayKey = start.getMonthOfYear() + "-" + start.getDayOfMonth();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getDayHourKeys(DateTime time) {
        return getDayHourKeys(time, daysHoursCount);
    }

    List<String> getDayHourKeys(DateTime time, int keysCount) {
        DateTime start = time.minusDays(keysCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusDays(1);
            String dayKey = start.getMonthOfYear() + "-" + start.getDayOfMonth();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getHoursKeys(DateTime time) {
        return getHoursKeys(time, hoursCount);
    }

    List<String> getHoursKeys(DateTime time, int keysCount) {
        DateTime start = time.minusHours(keysCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusHours(1);
            String dayKey = start.getDayOfMonth() + "-" + start.getHourOfDay();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getMinuteKeys(DateTime time) {
        return getMinuteKeys(time, minutesCount);
    }

    List<String> getMinuteKeys(DateTime time, int keysCount) {
        DateTime start = time.minusMinutes(keysCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusMinutes(1);
            String dayKey = start.getHourOfDay() + "-" + start.getMinuteOfHour();
            keys.add(dayKey);
        }
        return keys;
    }

    int getMinutesSinceLastUpdate(DateTime currentTime, TimeWindowDataAggregation aggregation) {
        return (int)((currentTime.getMillis() - aggregation.getUpdatedAt()) / 1000 ) / 60;
    }

    int getHoursSinceLastUpdate(DateTime currentTime, TimeWindowDataAggregation aggregation) {
        return (int)(((currentTime.getMillis() - aggregation.getUpdatedAt()) / 1000 ) / 60) / 60;
    }

    int getDaysSinceLastUpdate(DateTime currentTime, TimeWindowDataAggregation aggregation) {
        return (int)((((currentTime.getMillis() - aggregation.getUpdatedAt()) / 1000 ) / 60) / 60) / 24;
    }

    /**
     * @param <T> input object to process
     */
    public interface IAggregationUpdater<T> {

        /**
         * This method will be called with the data map, the key to update, and input event. Client can update the
         * "data" (with key) with any business logic
         */
        void update(StringObjectMap data, String key, T event);

        /**
         * Default behaviour - any keys out of the range will be deleted - client can override and can do some custom
         * logic.
         */
        default void expired(StringObjectMap data, String key, T event) {
            data.remove(key);
        }
    }

    /**
     * Configure aggregation helper class -
     *
     * <pre>
     * hourAggregationWindow - if > 0 then last "hourAggregationWindow" hours will be aggregated
     *              e.g. {"updated_at":1662020452476, "hours":{"1-13":3}}
     *
     * minuteAggregationWindow - if > 0 then last "minuteAggregationWindow" minutes will be aggregated
     *              e.g. {"updated_at":1662020452476, "minutes":{"13-50":3}}
     *
     * dayHourAggregationWindow - if > 0 then last "dayHourAggregationWindow" days will be aggregated
     *              e.g. {"updated_at":1662020452476, "days_hours":{"9-1":{"13":3}}}
     *
     * dayAggregationWindow - if > 0 then last "dayAggregationWindow" days will be aggregated
     *              e.g. {"updated_at":1662020452476, "days":{"9-1":2}}
     *
     *              NOTE - if "dayHourAggregationWindow" is > 0 then "dayAggregationWindow" is ignored
     * </pre>
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Builder
    public static class Config {
        private int dayAggregationWindow;
        private int dayHourAggregationWindow;
        private int hourAggregationWindow;
        private int minuteAggregationWindow;
    }
}