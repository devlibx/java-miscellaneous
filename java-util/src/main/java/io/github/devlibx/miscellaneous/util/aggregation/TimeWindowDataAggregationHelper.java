package io.github.devlibx.miscellaneous.util.aggregation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

import java.util.ArrayList;
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

    public void processDayHour(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (daysHoursCount <= 0) return;

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getDayHourKeys(currentTime);
        Set<String> keysInInput = aggregation.getDaysHours().keySet();
        keysInInput.forEach(keysToRemove::remove);
        keysToRemove.forEach(keyToRemove -> updater.expired(aggregation.getDaysHours(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getMonthOfYear() + "-" + eventTime.getDayOfMonth();
        if (getDayHourKeys(currentTime).contains(key)) {
            if (!aggregation.getDaysHours().containsKey(key)) {
                aggregation.getDaysHours().put(key, new StringObjectMap());
            }
            StringObjectMap dayData = aggregation.getDaysHours().getStringObjectMap(key);
            updater.update(dayData, eventTime.getHourOfDay() + "", event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    public void processDay(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (daysCount <= 0) return;

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getDayKeys(currentTime);
        Set<String> keysInInput = aggregation.getDays().keySet();
        keysInInput.forEach(keysToRemove::remove);
        keysToRemove.forEach(keyToRemove -> updater.expired(aggregation.getDays(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getMonthOfYear() + "-" + eventTime.getDayOfMonth();
        if (getDayKeys(currentTime).contains(key)) {
            updater.update(aggregation.getDays(), key, event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    public void processHours(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (hoursCount <= 0) return;

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getHoursKeys(currentTime);
        Set<String> keysInInput = aggregation.getHours().keySet();
        keysInInput.forEach(keysToRemove::remove);
        keysToRemove.forEach(keyToRemove -> updater.expired(aggregation.getHours(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getDayOfMonth() + "-" + eventTime.getHourOfDay();
        if (getHoursKeys(currentTime).contains(key)) {
            updater.update(aggregation.getHours(), key, event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    public void processMinutes(TimeWindowDataAggregation aggregation, DateTime currentTime, T event, DateTime eventTime, IAggregationUpdater<T> updater) {
        if (minutesCount <= 0) return;

        // Find what are the expired keys in the input aggregation
        List<String> keysToRemove = getMinuetKeys(currentTime);
        Set<String> keysInInput = aggregation.getMinutes().keySet();
        keysInInput.forEach(keysToRemove::remove);
        keysToRemove.forEach(keyToRemove -> updater.expired(aggregation.getMinutes(), keyToRemove, event));

        // Call updater to add data for give day
        String key = eventTime.getHourOfDay() + "-" + eventTime.getMinuteOfHour();
        if (getMinuetKeys(currentTime).contains(key)) {
            updater.update(aggregation.getMinutes(), key, event);
        }

        aggregation.setUpdatedAt(DateTime.now().getMillis());
    }

    List<String> getDayKeys(DateTime time) {
        DateTime start = time.minusDays(daysCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusDays(1);
            String dayKey = start.getMonthOfYear() + "-" + start.getDayOfMonth();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getDayHourKeys(DateTime time) {
        DateTime start = time.minusDays(daysHoursCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusDays(1);
            String dayKey = start.getMonthOfYear() + "-" + start.getDayOfMonth();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getHoursKeys(DateTime time) {
        DateTime start = time.minusHours(hoursCount);
        List<String> keys = new ArrayList<>();
        while (start.isBefore(time)) {
            start = start.plusHours(1);
            String dayKey = start.getDayOfMonth() + "-" + start.getHourOfDay();
            keys.add(dayKey);
        }
        return keys;
    }

    List<String> getMinuetKeys(DateTime time) {
        DateTime start = time.minusMinutes(minutesCount);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            start = start.plusMinutes(1);
            String dayKey = start.getHourOfDay() + "-" + start.getMinuteOfHour();
            keys.add(dayKey);
        }
        return keys;
    }

    interface IAggregationUpdater<T> {
        void update(StringObjectMap data, String key, T event);

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
