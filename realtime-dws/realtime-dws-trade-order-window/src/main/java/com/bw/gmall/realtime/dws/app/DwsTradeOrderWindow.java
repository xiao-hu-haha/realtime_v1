package com.bw.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.base.BaseApp;
import com.bw.gmall.realtime.common.bean.TradeOrderBean;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.function.DorisMapFunction;
import com.bw.gmall.realtime.common.util.DateFormatUtil;
import com.bw.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.crypto.MacSpi;
import java.time.Duration;

public class DwsTradeOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,"1",1,10027);
    }
  @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
      SingleOutputStreamOperator<String> map = stream
              .map(JSONObject::parseObject)
              .assignTimestampsAndWatermarks(
                      WatermarkStrategy
                              .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                              .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                              .withIdleness(Duration.ofSeconds(120L))
              )
              .keyBy(obj -> obj.getString("user_id"))
              .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                  private ValueState<String> lastOrderDateState;

                  @Override
                  public void open(Configuration parameters) {
                      lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDate", String.class));
                  }

                  @Override
                  public void processElement(JSONObject value,
                                             Context ctx,
                                             Collector<TradeOrderBean> out) throws Exception {
                      long ts = value.getLong("ts") * 1000;

                      String today = DateFormatUtil.tsToDate(ts);
                      String lastOrderDate = lastOrderDateState.value();

                      long orderUu = 0L;
                      long orderNew = 0L;
                      if (!today.equals(lastOrderDate)) {
                          orderUu = 1L;
                          lastOrderDateState.update(today);

                          if (lastOrderDate == null) {
                              orderNew = 1L;
                          }

                      }
                      if (orderUu == 1) {
                          out.collect(new TradeOrderBean("", "", "", orderUu, orderNew, ts));
                      }
                  }
              })
              .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
              .reduce(
                      new ReduceFunction<TradeOrderBean>() {
                          @Override

                          public TradeOrderBean reduce(TradeOrderBean value1,
                                                       TradeOrderBean value2) {
                              value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                              value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                              return value1;
                          }
                      },
                      new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                          @Override
                          public void process(Context ctx,
                                              Iterable<TradeOrderBean> elements,
                                              Collector<TradeOrderBean> out) throws Exception {
                              TradeOrderBean bean = elements.iterator().next();
                              bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                              bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                              bean.setCurDate(DateFormatUtil.tsToDateForPartition(System.currentTimeMillis()));

                              out.collect(bean);
//                              System.out.println(bean+"=============================>");
                          }
                      }
              )
              .map(new DorisMapFunction<>());
          map.print();
          map.sinkTo(FlinkSinkUtil.getDorisSink("ch_gmall_env.dws_trade_order_window"));


  }
}
