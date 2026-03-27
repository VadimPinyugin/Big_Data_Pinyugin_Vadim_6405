package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String faresFile = params.get("fares", pathToFareData);

		final int maxEventDelay = 60;
		final int servingSpeedFactor = 600;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(parallelism);

		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMaxTips = fares
				.keyBy(fare -> fare.driverId)
				.timeWindow(Time.hours(1))
				.aggregate(new SumTips(), new HourlyTipsWindowFunction())
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		printOrTest(hourlyMaxTips);

		env.execute("Hourly Tips");
	}

	public static class SumTips implements AggregateFunction<TaxiFare, Float, Float> {
		@Override
		public Float createAccumulator() {
			return 0f;
		}

		@Override
		public Float add(TaxiFare fare, Float accumulator) {
			return accumulator + fare.tip;
		}

		@Override
		public Float getResult(Float accumulator) {
			return accumulator;
		}

		@Override
		public Float merge(Float a, Float b) {
			return a + b;
		}
	}

	public static class HourlyTipsWindowFunction extends ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long key, Context context, Iterable<Float> tips, Collector<Tuple3<Long, Long, Float>> out) {
			out.collect(Tuple3.of(context.window().getEnd(), key, tips.iterator().next()));
		}
	}
}