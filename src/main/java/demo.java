import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class demo {
    public static void main(String args[]) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  通过socket获取源数据
        DataStreamSource<String> sourceData = env.readTextFile("E:/mingyao/code/flinkdemo1/word.txt");
        //env.socketTextStream("10.0.6.68", 9000);

        /**
         *  数据源进行处理
         *  flatMap方法与spark一样，对数据进行扁平化处理
         *  将每行的单词处理为<word,1>
         */
        DataStream<Tuple2<String, Integer>> dataStream = sourceData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                // 相同的单词进行分组
                .keyBy(0)
                //  聚合数据
                .sum(1);
        //  将数据流打印到控制台
        dataStream.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}

