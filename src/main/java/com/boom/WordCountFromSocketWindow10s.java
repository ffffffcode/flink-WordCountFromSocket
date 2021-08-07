package com.boom;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCountFromSocketWindow10s {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new Tokenizer()).keyBy(r -> r.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new WindowCollector());
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

        // 会覆盖原始数据
        sum.addSink(new RedisSink<>(jedisPoolConfig, new WordRedisMapper()));

        sum.print();

        env.execute("WordCountFromSocketWindow10s");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] stringList = value.split("\\s");
            for (String s : stringList) {
                // 使用out.collect方法向下游发送数据
                out.collect(Tuple2.of(s, 1));
            }
        }
    }

    private static class WordRedisMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "flink:work");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return stringIntegerTuple2.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return stringIntegerTuple2.f1.toString();
        }
    }

    private static class WindowCollector extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> in, Collector<Tuple2<String, Integer>> out) {
            AtomicInteger count = new AtomicInteger();
            in.forEach(stringIntegerTuple2 -> {
                count.addAndGet(stringIntegerTuple2.f1);
            });
            out.collect(Tuple2.of(key, count.get()));
        }
    }
}