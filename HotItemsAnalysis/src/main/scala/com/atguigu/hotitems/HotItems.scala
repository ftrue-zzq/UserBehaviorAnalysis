package com.atguigu.hotitems

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, count: Long, windowEnd: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //创建环境及配置
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取数据并转换成样例类类型，并且提取时间戳设置watermark
    //val inputStream: DataStream[UserBehavior] = env
    //  .readTextFile("E:\\IdeaProjects\\Practice-0317\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop202:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))


    val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val linearray = line.split(",")
      UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000)
    //测试dataStream
    //dataStream.print("data")

    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId") //按照商品ID分组
      .timeWindow(Time.minutes(60), Time.minutes(5)) //开窗，1小时、每5分钟滑动一次的滑动窗口
      .aggregate(new CountAgg(), new ItemCountWindowResult())
    //测试aggStream
    //aggStream.print("agg")

    //按窗口分组，排序输出TopN
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItemsResult(3))  //求点击量前3名的商品
    //测试resultStream
    resultStream.print("result")


    env.execute("Hot Items Job")
  }
}


//实现自定义的预聚合函数
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  //每来一个元素，聚合状态加一
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  //在session window中才会涉及到此方法
  override def merge(a: Long, b: Long): Long = a + b
}


//实现自定义的窗口函数
class ItemCountWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0  //注意java的Tuple和scala的Tuple
    val windowEnd: Long = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, count, windowEnd))
  }
}


//实现自定义的KeyedProcessFunction
class TopNHotItemsResult(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //定义一个列表状态，用来保存当前窗口内所有商品的count统计结果
  private var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //命名状态变量的名字和状态变量的类型
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount])
    //定义状态变量
    itemViewCountListState = getRuntimeContext.getListState(itemStateDesc)
  }

  //每来一个数据，就把它加入到ListState里
  override def processElement(input: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    itemViewCountListState.add(input)
    //注册一个定时器，windowEnd + 100触发
    ctx.timerService().registerEventTimeTimer(input.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //先将状态提取出来，用一个List保存
    import scala.collection.JavaConversions._ //否则 toList 报错
    val allItemViewCountList: List[ItemViewCount] = itemViewCountListState.get().toList

    //提前清空状态
    itemViewCountListState.clear()

    //排序并取TopN
    val topNHotItemViewCountList = allItemViewCountList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //排名信息格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历topN列表，逐个输出
    for (i <- topNHotItemViewCountList.indices) {
      val currentItemViewCount: ItemViewCount = topNHotItemViewCountList(i)
      result.append("NO.").append(i + 1).append(":")
        .append("\t 商品ID = ").append(currentItemViewCount.itemId)
        .append("\t 热门度 = ").append(currentItemViewCount.count)
        .append("\n")
    }

    result.append("\n ======================================== \n\n")

    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}












