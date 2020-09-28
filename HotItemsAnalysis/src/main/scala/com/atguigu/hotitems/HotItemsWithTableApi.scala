package com.atguigu.hotitems

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HotItemsWithTableApi {
  def main(args: Array[String]): Unit = {
    //创建环境及配置
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据并转换成样例类类型，并且提取时间戳设置watermark
    val inputStream: DataStream[String] = env.readTextFile("E:\\IdeaProjects\\Practice-0317\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)

    //表环境的创建
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //将DataStream转换成表
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //基于Table API进行窗口聚合
    val aggTable: Table = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'itemId.count as 'cnt, 'sw.end as 'windowEnd)
    //测试aggTable
    //aggTable.toAppendStream[Row].print("agg")

    //SQL排序输出
    tableEnv.createTemporaryView("agg", aggTable, 'itemId, 'cnt, 'windowEnd)
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from (
        | select *,
        |   row_number() over (partition by windowEnd order by cnt desc) as row_num
        |  from agg
        |)
        |where row_num <= 5
        |""".stripMargin
    )

    resultTable.toRetractStream[Row].print("res")


    env.execute("hot items with table api")
  }
}
