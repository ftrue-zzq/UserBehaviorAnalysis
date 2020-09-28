package com.atguigu.networkflow

import java.net.URL

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

//case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class UvCount(timestamp: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    //获取并设置执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    //将输入数据的类型转换成样例类类型, 并提取TimeStamp、设置Watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //开窗  去重  聚合
    val resultStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      //.apply(new UvCountResult)  //方法一：没有增量聚合
      .aggregate(new UvCountAgg, new UvCountAggResult)  //方法二：增量聚合 更好（速度更快）

    resultStream.print("result")

    env.execute("uv job")
  }
}

//实现自定义的WindowFunction
class UvCountResult extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //用一个set集合类型，来保存所有的userId，自动去重
    //val s: mutable.Set[Long] = collection.mutable.Set()
    var idSet = Set[Long]()

    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }

    //输出set的大小
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}

class UvCountAgg extends AggregateFunction[UserBehavior, Set[Long], Long] {
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

class UvCountAggResult extends AllWindowFunction[Long, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect(UvCount(window.getEnd, input.head))
  }
}
