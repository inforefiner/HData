package com.github.stuxuhai.hdata.core;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.github.stuxuhai.hdata.api.Metric;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.RecordCollector;
import com.github.stuxuhai.hdata.transform.UDF;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DefaultRecordCollector implements RecordCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRecordCollector.class);

    private static final long SLEEP_MILL_SECONDS = 1000;

    private final DefaultStorage storage;
    private final Metric metric;
    private final long flowLimit;
    private Stopwatch stopwatch = Stopwatch.createStarted();
    private Map<Integer, String> udfMap;
    private UDF udfList = new UDF();
    private String encryptKey;

    public DefaultRecordCollector(DefaultStorage storage, Metric metric, long flowLimit, Map<Integer, String> udfMap,
                                  String encryptKey) {
        this.storage = storage;
        this.metric = metric;
        this.flowLimit = flowLimit;
        this.udfMap = udfMap;
        this.encryptKey = encryptKey;
        LOGGER.info("The flow limit is {} bytes/s.", this.flowLimit);
    }

    @Override
    public void send(Record record) {
        // 限速
        if (flowLimit > 0) {
            while (true) {
                long currentSpeed = metric.getSpeed();
                if (currentSpeed > flowLimit) {
                    if (stopwatch.elapsed(TimeUnit.SECONDS) >= 5) {
                        LOGGER.info("Current Speed is {} MB/s, sleeping...", String.format("%.2f", (double) currentSpeed / 1024 / 1024));
                        stopwatch.reset();
                    }
                    Utils.sleep(SLEEP_MILL_SECONDS);
                } else {
                    break;
                }
            }
        }
        if (udfMap != null && udfMap.size() > 0) {
            record = doTransform(record);
        }
        storage.put(record);
        metric.getReadCount().incrementAndGet();

        if (flowLimit > 0) {
            metric.getReadBytes().addAndGet(RamUsageEstimator.sizeOf(record));
        }

    }

    @Override
    public void send(Record[] records) {
        //storage.put(records);
        for (Record record : records) {
            send(record);
        }
    }

    public Record doTransform(Record record) {
        Record _record = new DefaultRecord(record.size());
        int idx = -1;
        for (int i = 0; i < record.size(); i++) {
            Object obj = record.get(i);
            if (udfMap.containsKey(i)) {
                String udf = udfMap.get(i);
                switch (udf) {
                    case "blank":
                        obj = udfList.blank(obj);
                        break;
                    case "mix":
                        obj = udfList.mix(obj);
                        break;
                    case "encrypt": //加密
                        obj = udfList.encrypt(obj, encryptKey);
                        break;
                    case "checksum": //校验和
                        idx = i;     //记录需要做校验和字段的index, 对其他字段全部transform完成后,再计算校验和
                        break;
                }
            }
            _record.add(i, obj);
        }
        //如果需要,计算校验和
        if (idx != -1) {
            Object obj = udfList.checksum(_record.values(), idx);
            _record.add(idx, obj);
        }
//        LOGGER.info("record is : " + Arrays.toString(_record.values()));
        return _record;
    }
}
