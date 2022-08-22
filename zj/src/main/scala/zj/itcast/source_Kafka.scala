package zj.itcast

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import java.util.Date
import java.util.Properties

case class WaterSensor(word: String, num: Int){

}
object source_Kafka {
  def main(args: Array[String]): Unit = {

    //1.创建实时流处理的环境
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment

    //设置kafka所需要的配置项
    val prop: Properties = new Properties()

    // 2、指定Kafka集群主机名和端口号
    prop.setProperty("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")

    // 3、指定消费者组ID,在同一时刻同一消费组中只有
    // 一个线程可以去消费一个分区消息， 不同的消费组可以去消费同一个分区的消息。
    prop.setProperty("group.id","myTopic")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //获取现在日期
    val date = new Date()

    //source------------------------------------------------------------------------------------------------------------
    //从kafka读取数据源生成DataStream
    val dataStream_kafka: DataStream[String] = env
      .addSource(new FlinkKafkaConsumer[String]("myTopic", new SimpleStringSchema(), prop))
    //输出获取到的数据
    dataStream_kafka.print("输入内容为")

    val low = dataStream_kafka.map(_.toLowerCase)
    low.print("转换小写")

    //split-------------------------kafka输入的数据要用逗号隔开！！！！-----------------------------------------------------
    val split: DataStream[(String, Int)] = low
      .assignTimestampsAndWatermarks(new TimestampExtractor)
      .flatMap(_.split(","))
      .map(_ -> 1)
    split.print("切分操作")


    //sink--------------------------------------------------------------------------------------------------------------
    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path("E:\\intellij_wenjian\\zj\\out"),
        new SimpleStringEncoder[String]()).build()
    dataStream_kafka.addSink(sink)

    //map---------------------------------------------------------------------------------------------------------------
    val map: DataStream[String] = dataStream_kafka
      .map(new MapFunction[String, String] {
        override def map(t: String): String = {
          t * 2
        }
      })
    map.print("map算子操作乘2")

    //keyby+滚动窗口统计5秒内的数据-----------------------------------------------------------------------------------------------------
    split
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows
        .of(Time.seconds(5)))
      .sum(1)
      .print("-----------------------------------统计5s内数据-----------------------------------")


    //reduce
    val reduce: DataStream[(String, Int)] = split
      .keyBy(_._1)
      .reduce((t1, t2) => Tuple2.apply(t1._1, t1._2 + t2._2))
    reduce.print(date.toString + " " +"reduce统计全部")

    //-------------------------------------------***********运行************--------------------------------------------

    //每个5秒执行一次---AT_LEAST_ONCE---
    env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE)

    //设置全局并行度为1
    //输出结果
    env.setParallelism(1)

    //retract
    //创建表执行环境
    val table: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,table)


    //接收指定端口得数据，并转换成样例类WaterSensor类型的DataStream
    val dataStream: DataStream[WaterSensor] = low
      .map(a=>{
        val strings: Array[String] = a.split(",")
        WaterSensor(strings(0),strings(1).toInt)
      })

    //    //根据流创建一张Table类型得得对象
    tableEnv.registerDataStream(
      "Table1",
      dataStream,
      'word, 'num
    )

    //调用Table API进行转换
    val dataTable2: Table = tableEnv.sqlQuery(" SELECT cnt, count(word) AS freq FROM (SELECT word, count(num) AS cnt FROM Table1 GROUP BY word) GROUP BY cnt ")

    //使用追加模式，当有数据更新时，直接在后面跟着输出
    tableEnv.toRetractStream[Row](dataTable2)
      .print("retract")


    //2.流处理必须添加开启启动执行的语句
    env.execute()

  }

  /**
   * 时间戳提取器需要实现AssignerWithPeriodicWatermarks;
   *
   */
  class TimestampExtractor extends AssignerWithPeriodicWatermarks[String] with Serializable {
    private val maxOutOfOrderness = 5000L //允许数据晚到的最大时间5s;
    private var wm: Watermark = _

    override def getCurrentWatermark: Watermark = {
      wm = new Watermark(System.currentTimeMillis() - maxOutOfOrderness)
      wm
    }
    override def extractTimestamp(t: String, l: Long): Long = {
      l
    }

  }
}
