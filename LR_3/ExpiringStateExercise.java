package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ExpiringStateExercise extends ExerciseBase {
    static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
    static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
        final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
                .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
                .keyBy(ride -> ride.rideId);

        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
                .keyBy(fare -> fare.rideId);

        SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>> processed = rides
                .connect(fares)
                .process(new EnrichmentFunction());

        printOrTest(processed.getSideOutput(unmatchedFares));

        env.execute("ExpiringStateExercise");
    }

    public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            // Настройка TTL - состояние будет автоматически удалено через 5 минут
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(5))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<TaxiRide> rideDescriptor = new ValueStateDescriptor<>("saved ride", TaxiRide.class);
            rideDescriptor.enableTimeToLive(ttlConfig);
            rideState = getRuntimeContext().getState(rideDescriptor);

            ValueStateDescriptor<TaxiFare> fareDescriptor = new ValueStateDescriptor<>("saved fare", TaxiFare.class);
            fareDescriptor.enableTimeToLive(ttlConfig);
            fareState = getRuntimeContext().getState(fareDescriptor);
        }

        @Override
        public void processElement1(TaxiRide ride, Context ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                out.collect(new Tuple2<>(ride, fare));
            } else {
                rideState.update(ride);
                // Устанавливаем таймер для очистки, если не придет оплата
                ctx.timerService().registerEventTimeTimer(ride.getEventTime() + 300000);
            }
        }

        @Override
        public void processElement2(TaxiFare fare, Context ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                out.collect(new Tuple2<>(ride, fare));
            } else {
                fareState.update(fare);
                // Устанавливаем таймер для очистки, если не придет поездка
                ctx.timerService().registerEventTimeTimer(fare.getEventTime() + 300000);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            if (rideState.value() != null) {
                ctx.output(unmatchedRides, rideState.value());
                rideState.clear();
            }
            if (fareState.value() != null) {
                ctx.output(unmatchedFares, fareState.value());
                fareState.clear();
            }
        }
    }
}