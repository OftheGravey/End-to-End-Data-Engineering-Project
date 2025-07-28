package com.extractor.flink.functions;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public abstract class FactDimensionJoiner<FACT extends DebeziumSourceRecord, DIMENSION extends TargetDimensionRecord, ENRICHEDFACT> extends KeyedCoProcessFunction<Integer, FACT, DIMENSION, ENRICHEDFACT> {
        // Store all facts not yet processed
        private transient ListState<FACT> watingFactState;
        // Store all active dimensions
        private transient MapState<Long, DIMENSION> activeDimensionState;

        private final TypeInformation<FACT> factTypeInfo;
        private final TypeInformation<DIMENSION> dimensionTypeInfo;

        protected FactDimensionJoiner(TypeInformation<FACT> factTypeInfo,
                                  TypeInformation<DIMENSION> dimensionTypeInfo) {
        this.factTypeInfo = factTypeInfo;
        this.dimensionTypeInfo = dimensionTypeInfo;
    }

        @Override
        public void open(OpenContext ctx) throws Exception {
            StateTtlConfig factTtl = StateTtlConfig.newBuilder(Duration.ofSeconds(30)).build();
            ListStateDescriptor<FACT> factListDescriptor =  new ListStateDescriptor<>("waitingFacts", factTypeInfo);
            factListDescriptor.enableTimeToLive(factTtl);

            watingFactState = getRuntimeContext().getListState(factListDescriptor);
            
            StateTtlConfig dimensionTtl = StateTtlConfig.newBuilder(Duration.ofMinutes(3)).build();
            MapStateDescriptor<Long, DIMENSION> dimensionMapDescriptor = new MapStateDescriptor<>("allDimensions", TypeInformation.of(Long.class), dimensionTypeInfo);
            dimensionMapDescriptor.enableTimeToLive(dimensionTtl);

            activeDimensionState = getRuntimeContext().getMapState(dimensionMapDescriptor);       
            }

        @Override
        public void processElement1(FACT fact, Context context, Collector<ENRICHEDFACT> out)
                throws Exception {
            boolean joined = false;
            
            for (Map.Entry<Long, DIMENSION> entry : activeDimensionState.entries()) {
                DIMENSION currentDimension = entry.getValue();
                if (isValidDimension(currentDimension, fact)) {
                    out.collect(createJoinedDimension(fact, currentDimension));
                    joined = true;
                    break;
                }
            }
            if (!joined) {
                watingFactState.add(fact);
            }
        }

        @Override
        public void processElement2(DIMENSION dimension, Context context, Collector<ENRICHEDFACT> out)
                throws Exception {
            // An author record arrived. Store it.
            activeDimensionState.put(dimension.validFrom.getTime(), dimension);
                
            Iterator<FACT> currentFacts = watingFactState.get().iterator();
            while (currentFacts.hasNext()) {
                FACT currentFact = currentFacts.next();
                if (isValidDimension(dimension, currentFact))
                    out.collect(createJoinedDimension(currentFact, dimension));
            }
        }

        private boolean isValidDimension(DIMENSION dimension, FACT fact) {
            return (fact.tsMs >= dimension.validFrom.getTime()) && (fact.tsMs < dimension.validTo.getTime());
        }

        public abstract ENRICHEDFACT createJoinedDimension(FACT FACT, DIMENSION order);
    }