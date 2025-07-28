package com.extractor.flink.functions;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.io.Serializable;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public abstract class SCD2ProcessFunction<IN extends DebeziumSourceRecord, OUT extends TargetDimensionRecord>
        extends KeyedProcessFunction<Integer, IN, OUT>{

    private static final long END_OF_TIME = 253402300799000L; // 9999-12-31 in milliseconds
    private static final long BUFFER_TIMEOUT_MS = 10 * 1000;

    public ValueState<OUT> currentRecordState;
    public MapState<Long, IN> pendingRecordsState;
    public ValueState<Long> timerState;

    @FunctionalInterface
    public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {}
    public final SerializableBiFunction<IN, Long, OUT> outputFactory;
    private final  TypeInformation<IN> inTypeInfo;
    private final  TypeInformation<OUT> outTypeInfo;

    protected SCD2ProcessFunction(TypeInformation<IN> inTypeInfo,
                                  TypeInformation<OUT> outTypeInfo,
                                  SerializableBiFunction<IN, Long, OUT> outputFactory) {
        this.inTypeInfo = inTypeInfo;
        this.outTypeInfo = outTypeInfo;
        this.outputFactory = outputFactory;
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        currentRecordState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("currentRecord", outTypeInfo)
        );

        pendingRecordsState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("pendingRecords", TypeInformation.of(Long.class), inTypeInfo)
        );

        timerState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("timer", Long.class)
        );
    }

    @Override
    public void processElement(IN record, Context ctx, Collector<OUT> out) throws Exception {
        pendingRecordsState.put(record.tsMs, record);

        long currentTime = ctx.timerService().currentProcessingTime();
        long timerTime = currentTime + BUFFER_TIMEOUT_MS;

        Long existingTimer = timerState.value();
        if (existingTimer == null || timerTime < existingTimer) {
            if (existingTimer != null) {
                ctx.timerService().deleteProcessingTimeTimer(existingTimer);
            }
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            timerState.update(timerTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        List<Map.Entry<Long, IN>> sortedRecords = new ArrayList<>();
        for (Map.Entry<Long, IN> entry : pendingRecordsState.entries()) {
            sortedRecords.add(entry);
        }

        sortedRecords.sort(Map.Entry.comparingByKey());

        OUT currentRecord = currentRecordState.value();
        for (int i = 0; i < sortedRecords.size(); i++) {
            IN record = sortedRecords.get(i).getValue();
            Long validTo = calculateValidTo(record, sortedRecords, i);

            if (currentRecord != null) {
                OUT newCurrentRecord = (OUT) currentRecord.clone(new Timestamp(record.tsMs - 1));
                newCurrentRecord.validTo = new Timestamp(record.tsMs - 1);
                out.collect(newCurrentRecord);
            }

            OUT scd2Record = outputFactory.apply(record, END_OF_TIME);


            if ("d" != (record.op)) {
                currentRecordState.update(scd2Record);
                currentRecord = scd2Record;
            } else {
                currentRecordState.clear();
                currentRecord = null;
            }
            out.collect(scd2Record);
        }
        pendingRecordsState.clear();
        timerState.clear();
    }

    private Long calculateValidTo(IN currentRecord, List<Map.Entry<Long, IN>> sortedRecords,
            int currentIndex) {
        if ("d".equals(currentRecord.op) || currentIndex == sortedRecords.size() - 1) {
            return END_OF_TIME;
        } else {
            IN nextRecord = sortedRecords.get(currentIndex + 1).getValue();
            return nextRecord.tsMs - 1;
        }
    }
}