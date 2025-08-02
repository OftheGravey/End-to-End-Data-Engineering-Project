package com.extractor.flink.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// Function for consolidating record closures
public class SCD2MostValidFunction<Event extends TargetDimensionRecord> extends KeyedProcessFunction<String, Event, Event> {
    private static final long END_OF_TIME = 253402300799000L;
	private transient ValueState<Event> latestEventState;
	private final TypeInformation<Event> eventTypeInfo;
    private transient ValueState<Long> timerState;
    public long timeWindowSizeMs = 100 * 1000L; // 10 second default window

	public SCD2MostValidFunction(TypeInformation<Event> eventTypeInfo) {
		this.eventTypeInfo = eventTypeInfo;
	}

	@Override
    public void open(OpenContext ctx) throws Exception {
        this.latestEventState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("latestEvent", eventTypeInfo)
        );
        this.timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerState", Long.class)
        );
    }

	@Override
	public void processElement(Event record, Context ctx, Collector<Event> out) throws Exception {
        Long  currentTimer = timerState.value();

        if (record.validTo != END_OF_TIME) {
            out.collect(record);
            if (currentTimer != null) {
                ctx.timerService().deleteEventTimeTimer(currentTimer);
            }   
            return;
        }

        latestEventState.update(record); 
        long newTimer = getAcceptanceWindow(record.validFrom, 0, timeWindowSizeMs) + timeWindowSizeMs;
        ctx.timerService().registerEventTimeTimer(newTimer);
        timerState.update(newTimer);
	}

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {
        Event current = latestEventState.value();
        if (current != null) {
            out.collect(current);
            latestEventState.clear();
        }
    }

    private long getAcceptanceWindow(long timestamp, long offset, long windowSize) {
        final long reminder = (timestamp - offset) % windowSize;
        if (reminder < 0) { 
            return timestamp - (reminder + windowSize);
        }
        return timestamp - reminder;
    }
}
