package com.example.gmall_api.mapper;



import com.example.gmall_api.bean.TradeProvinceOrderAmount;
import com.example.gmall_api.bean.TradeProvinceOrderCt;
import com.example.gmall_api.bean.TradeStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TradeStatsMapper {
    // 交易总金额
    @Select("select sum(order_amount) order_total_amount\n" +
            "from dws_trade_province_order_window\n" +
            "         ")
    Double selectTotalAmount(@Param("date") Integer date);

    // 下单情况统计
    @Select("select '订单数' type,\n" +
            "       sum(order_count) value\n" +
            "from dws_trade_province_order_window\n" +
            "\n" +
            "union all\n" +
            "select '下单人数' type,\n" +
            "       sum(order_unique_user_count) value\n" +
            "from dws_trade_order_window\n" +
            "\n" +
            "union all\n" +
            "select '退单数' type,\n" +
            "       sum(refund_count)       value\n" +
            "from dws_trade_trademark_category_user_refund_window\n" +
            " \n" +
            "union all\n" +
            "select '退单人数'                  type,\n" +
            "       count(distinct user_id) value\n" +
            "from dws_trade_trademark_category_user_refund_window\n" +
            " ")
    List<TradeStats> selectTradeStats(@Param("date") Integer date);


    @Select("select province_name,\n" +
            "       sum(order_count) order_count\n" +
            "from dws_trade_province_order_window\n" +
            "group by province_id, province_name;")
    List<TradeProvinceOrderCt> selectTradeProvinceOrderCt(@Param("date")Integer date);

    @Select("select province_name,\n" +
            "       sum(order_amount) order_amount\n" +
            "from dws_trade_province_order_window\n" +
            "group by province_id, province_name;")
    List<TradeProvinceOrderAmount> selectTradeProvinceOrderAmount(@Param("date")Integer date);

}