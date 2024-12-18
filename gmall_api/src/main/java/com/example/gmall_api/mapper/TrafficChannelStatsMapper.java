package com.example.gmall_api.mapper;



import com.example.gmall_api.bean.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficChannelStatsMapper {

    // 1. 获取各渠道独立访客数
    @Select("SELECT\n" +
            "\tch,\n" +
            "\tsum( uv_ct ) uv_ct \n" +
            "FROM\n" +
            "\tdws_traffic_vc_ch_ar_is_new_page_view_window \n" +
            "GROUP BY\n" +
            "\tch \n" +
            "ORDER BY\n" +
            "\tuv_ct DESC")
    List<TrafficUvCt> selectUvCt(@Param("date") Integer date);

    // 2. 获取各渠道会话数
    @Select("select ch,\n" +
            "       sum(sv_ct) sv_ct\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
//            "         partition (par#{date})\n" +
            "group by ch\n" +
            "order by sv_ct desc;")
    List<TrafficSvCt> selectSvCt(@Param("date") Integer date);

    // 3. 获取各渠道会话平均页面浏览数
    @Select("select ch,\n" +
            "       sum(pv_ct) / sum(sv_ct) pv_per_session\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
//            "         partition (par#{date})\n" +
            "group by ch\n" +
            "order by pv_per_session desc;")
    List<TrafficPvPerSession> selectPvPerSession(@Param("date") Integer date);

    // 4. 获取各渠道会话平均页面访问时长
    @Select("select ch,\n" +
            "       sum(dur_sum) / sum(sv_ct) dur_per_session\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
//            "         partition (par#{date})\n" +
            "group by ch\n" +
            "order by dur_per_session desc;")
    List<TrafficDurPerSession> selectDurPerSession(@Param("date") Integer date);

    // 5. 获取各渠道跳出率
    @Select("select ch,\n" +
            "       sum(uv_ct) / sum(sv_ct) uj_rate\n" +
            "from dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
//            "         partition (par#{date})\n" +
            "group by ch\n" +
            "order by uj_rate desc;")
    List<TrafficUjRate> selectUjRate(@Param("date") Integer date);





}



