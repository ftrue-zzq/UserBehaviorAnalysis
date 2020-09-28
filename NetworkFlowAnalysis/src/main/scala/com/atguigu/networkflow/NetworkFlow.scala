package com.atguigu.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 输入及窗口聚合结果样例类
case class WebServerLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, count: Long, windowEnd: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    //获取执行环境并进行配置
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取数据
    //val inputStream: DataStream[String] = env.readTextFile("E:\\IdeaProjects\\Practice-0317\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    //将输入数据转换成所需的样例类类型
    val dataStream: DataStream[WebServerLogEvent] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(" ")
        // 从日志数据中提取时间字段，并转换成时间戳
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = sdf.parse(arr(3)).getTime
        WebServerLogEvent(arr(0), arr(1), timestamp, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WebServerLogEvent](Time.seconds(3)) {
        override def extractTimestamp(element: WebServerLogEvent): Long = element.timestamp
      })
    //测试dataSTream
    dataStream.print("data")

    val lateTag = new OutputTag[WebServerLogEvent]("late")

    //开窗聚合
    val aggStream: DataStream[PageViewCount] = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(60), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateTag)
      .aggregate(new PageCountAgg(), new PageCountWindowResult())
    //测试aggStream
    aggStream.print("agg")
    //测试sideoutputStream
    val sideoutputStream: DataStream[WebServerLogEvent] = aggStream.getSideOutput(lateTag)
    sideoutputStream.print("late")

    //排序输出
    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNPageResult(3))

    resultStream.print("result")


    env.execute("networkflow job")
  }
}

class PageCountAgg() extends AggregateFunction[WebServerLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: WebServerLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, input.head, window.getEnd))
  }
}

//实现自定义ProcessFunction，进行排序输出
class TopNPageResult(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  //定义一个列表状态（两种方式）
  //方式1
  //private var pageViewCountListState: ListState[PageViewCount] = _
  //
  //override def open(parameters: Configuration): Unit = {
  //  val urlStateDesc: ListStateDescriptor[PageViewCount] = new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount])
  //  pageViewCountListState = getRuntimeContext.getListState(urlStateDesc)
  //}
  //方式2
  //lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))

  //为了对于同一个key进行更新操作，定义映射状态
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //pageViewCountListState.add(value)
    pageViewCountMapState.put(value.url, value.count)
    //注册一个定时器，100毫秒后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    //注册一个1分钟之后的定时器，用于清理状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
      //pageViewCountListState.clear()
      pageViewCountMapState.clear()
      return
    }

    //获取状态中的所有窗口聚合结果(两种方式）
    //方式1
    //import scala.collection.JavaConversions._
    //val allPageViewCounts: List[PageViewCount] = pageViewCountListState.get().toList
    //方式2
    //val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    //val iter: util.Iterator[PageViewCount] = pageViewCountListState.get().iterator()
    //while (iter.hasNext) {
    //  allPageViewCounts += iter.next()
    //}
    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    //提前清除状态
    //pageViewCountListState.clear()

    //排序取top n
    //1
    //val topNHotPageViewCounts: List[PageViewCount] = allPageViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(n)
    //2
    //val topNHotPageViewCounts: ListBuffer[PageViewCount] = allPageViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(n)
    val topNHotPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(n)

    //排名信息格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 100)).append("\n")
    //遍历topN列表，逐个输出
    for (i <- topNHotPageViewCounts.indices) {
      val currentItemViewCount = topNHotPageViewCounts(i)
      result.append("NO.").append(i + 1).append(":")
        .append("\t 页面 URL = ").append(currentItemViewCount._1)
        .append("\t 热门度 = ").append(currentItemViewCount._2)
        .append("\n")
    }

    result.append("\n ======================================== \n\n")

    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
