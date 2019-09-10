package com.inforefiner.hdata.reporter;

import com.codahale.metrics.Timer;
import com.codahale.metrics.*;
import com.merce.woven.data.rpc.DataService;

import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Simple reporter for send meter to data hub via dubbo.
 * Now only support meter, would support more metrics in future if needed.
 */
public class DubboReporter extends ScheduledReporter {

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;

        private String tenantId;
        private String taskId;
        private String channelId;
        private DataService dataService;

        private Locale locale;
        private Clock clock;
        private TimeZone timeZone;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;
        private Set<MetricAttribute> disabledMetricAttributes;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.clock = Clock.defaultClock();
            this.timeZone = TimeZone.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
            disabledMetricAttributes = Collections.emptySet();
        }

        /**
         * Specifies whether or not, the executor (used for reporting) will be stopped with same time with reporter.
         * Default value is true.
         * Setting this parameter to false, has the sense in combining with providing external managed executor via {@link #scheduleOn(ScheduledExecutorService)}.
         *
         * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
         * @return {@code this}
         */
        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        /**
         * Specifies the executor to use while scheduling reporting of metrics.
         * Default value is null.
         * Null value leads to executor will be auto created on start.
         *
         * @param executor the executor to use while scheduling reporting of metrics.
         * @return {@code this}
         */
        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Write to the given tenant Id.
         *
         * @param tenantId a {@link String} instance.
         * @return {@code this}
         */
        public Builder withTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        /**
         * Write to the given task Id.
         *
         * @param taskId a {@link String} instance.
         * @return {@code this}
         */
        public Builder withTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Write to the given channel Id.
         *
         * @param channelId a {@link String} instance.
         * @return {@code this}
         */
        public Builder withChannelId(String channelId) {
            this.channelId = channelId;
            return this;
        }

        /**
         * Write to the given {@link DataService}.
         *
         * @param dataService a {@link DataService} instance.
         * @return {@code this}
         */
        public Builder outputTo(DataService dataService) {
            this.dataService = dataService;
            return this;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formattedFor(Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Use the given {@link TimeZone} for the time.
         *
         * @param timeZone a {@link TimeZone}
         * @return {@code this}
         */
        public Builder formattedFor(TimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Don't report the passed metric attributes for all metrics (e.g. "p999", "stddev" or "m15").
         * See {@link MetricAttribute}.
         *
         * @param disabledMetricAttributes a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
            this.disabledMetricAttributes = disabledMetricAttributes;
            return this;
        }

        /**
         * Builds a {@link ConsoleReporter} with the given properties.
         *
         * @return a {@link ConsoleReporter}
         */
        public DubboReporter build() {
            return new DubboReporter(registry,
                    tenantId,
                    taskId,
                    channelId,
                    dataService,
                    locale,
                    clock,
                    timeZone,
                    rateUnit,
                    durationUnit,
                    filter,
                    executor,
                    shutdownExecutorOnStop,
                    disabledMetricAttributes);
        }
    }

    private String tenantId;
    private String taskId;
    private String channelId;
    private final DataService dataService;
    private final Locale locale;
    private final Clock clock;
    private final DateFormat dateFormat;

    private DubboReporter(MetricRegistry registry,
                          String tenantId,
                          String taskId,
                          String channelId,
                          DataService dataService,
                          Locale locale,
                          Clock clock,
                          TimeZone timeZone,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          MetricFilter filter,
                          ScheduledExecutorService executor,
                          boolean shutdownExecutorOnStop,
                          Set<MetricAttribute> disabledMetricAttributes) {
        super(registry, "dubbo-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop, disabledMetricAttributes);
        this.tenantId = tenantId;
        this.taskId = taskId;
        this.channelId = channelId;
        this.dataService = dataService;
        this.locale = locale;
        this.clock = clock;
        this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT,
                DateFormat.MEDIUM,
                locale);
        dateFormat.setTimeZone(timeZone);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }
    }

    private void reportMeter(long timestamp, String name, Meter meter) {
        dataService.report(tenantId, taskId, channelId, meter.getCount(), meter.getOneMinuteRate(), meter.getFiveMinuteRate(), meter.getFifteenMinuteRate(), meter.getMeanRate(), timestamp);
    }
}
