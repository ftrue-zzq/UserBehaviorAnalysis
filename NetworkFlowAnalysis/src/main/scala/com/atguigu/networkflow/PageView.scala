package com.atguigu.networkflow

import java.net.URL

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//定义输入输出样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    val dataStraem: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //开窗统计
    val pvCountStream: DataStream[PvCount] = dataStraem
      .filter(_.behavior == "pv")
      //.map(data => ("pv", 1L))
      //.keyBy(_._1) //指定一个dummy key，所有数据都分到一组  //此处会导致数据倾斜问题
      .map(new MyMapper()) //自定义MapFunction，随机生成key
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg(), new PvCountResult())
    //测试aggStream
    //pvCountStream.print("agg")

    //把窗口内所有分组数据汇总起来
    val pvTotalStream: DataStream[PvCount] = pvCountStream
      .keyBy(_.windowEnd)
      //.sum("count")
      .process(new TotalPvCountResult())

    pvTotalStream.print("total")

    env.execute("PV job")
  }
}

//自定义MapFunction
class MyMapper extends MapFunction[UserBehavior, (String, Long)] {
  override def map(value: UserBehavior): (String, Long) = (Random.nextString(10), 1L)
}

class PvCountAgg extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PvCountResult extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}

//实现自定义的KeyedProcessFunction，实现所有分组数据的聚合叠加
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {
  //定义一个状态
  //private var currentTotalCountState: ValueState[Long] = _
  //override def open(parameters: Configuration): Unit = {
  //  val pvcountDesc: ValueStateDescriptor[PvCount] = new ValueStateDescriptor[Long]("pvcount", classOf[Long])
  //  currentTotalCountState = getRuntimeContext.getState(pvcountDesc)
  //}
  lazy val currentTotalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    //获取当前count总和
    val currentTotalCount = currentTotalCountState.value()
    //叠加当前数据的count值，更新状态
    currentTotalCountState.update(currentTotalCount + value.count)
    //注册定时器，100ms之后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    //所有key的count值都已聚合，直接输出结果
    out.collect(PvCount(ctx.getCurrentKey, currentTotalCountState.value()))
    //清空状态
    currentTotalCountState.clear()
  }
}

