package com.atguigu.market

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.api.java.tuple.{Tuple, Tuple2}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//输入数据的样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
//输出统计数据的样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)


//自定义测试数据源
class MarketingSimulatedEventSource extends RichSourceFunction[MarketingUserBehavior] {
  //是否正常运行的标识位
  var running = true
  //定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  val channelSet: Seq[String] = Seq("AppStore", "HuaweiStore", "Wechat", "Weibo")

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //定义一个生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L

    while (running && count < maxElements) {
      //所有字段随机生成
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(Random.nextInt(behaviorSet.size))
      val channel = channelSet(Random.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))

      count += 1

      Thread.sleep(20)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}


object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new MarketingSimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    val resultStream: DataStream[MarketViewCount] = dataStream
      .filter(_.behavior != "UNINSTALL") //过滤掉卸载行为
      .keyBy("channel", "behavior") //按照渠道和行为分组
      .timeWindow(Time.hours(1), Time.seconds(5)) //开滑动窗口进行统计
      .process(new MarketingCountByChannel)

    resultStream.print("result")

    env.execute("app marketing by channel job")
  }
}

class MarketingCountByChannel extends ProcessWindowFunction[MarketingUserBehavior, MarketViewCount, Tuple, TimeWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key.asInstanceOf[Tuple2[String, String]].f0
    val behavior = key.asInstanceOf[Tuple2[String, String]].f1
    val count = elements.size
    out.collect(MarketViewCount(start, end,channel, behavior, count))
  }
}

