package com.bw.gmall.realtime.common.constant;

public class  Constant {
    public static final String KAFKA_BROKERS = "hadoop101:9092";

    public static final String TOPIC_DB = "realtime_topic_db";
    public static final String TOPIC_LOG = "realtime_topic_log";

    public static final String MYSQL_HOST = "hadoop101";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "root";
    public static final String HBASE_NAMESPACE = "gmall_env";
    public static final String PROCESS_DATABASE = "gmall_config";
    public static final String PROCESS_DIM_TABLE_NAME = "table_process_dim";
    public static final String PROCESS_DWD_TABLE_NAME = "table_process_dwd";

    public static final String HBASE_ZOOKEEPER_QUORUM = "hadoop101";

    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/gmall_env?serverTimezone=Asia/Shanghai";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_BASE_LOG = "dwd_base_log";

    public static final String FENODES = "hadoop101:7030";
    public static final String DORIS_DATABASE = "gmall2023_realtime";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PASSWORD = "root";


    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";


    public static final String DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    public static final String DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    public static final String DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW = "dws_traffic_home_detail_page_view_window";
    public static final String DWS_USER_USER_LOGIN_WINDOW = "dws_user_user_login_window";
    public static final String DWS_USER_USER_REGISTER_WINDOW = "dws_user_user_register_window";
    public static final String DWS_TRADE_CART_ADD_UU_WINDOW = "dws_trade_cart_add_uu_window";
    public static final String DWS_TRADE_SKU_ORDER_WINDOW = "dws_trade_sku_order_window";
    public static final String DWS_TRADE_ORDER_WINDOW = "dws_trade_order_window";
    public static final String DWS_TRADE_PAYMENT_SUC_WINDOW = "dws_trade_payment_suc_window";
    public static final String DWS_TRADE_SKU_ORDER_USER_TAG_WINDOW = "dws_trade_sku_order_user_tage_window";
    public static final String DWS_TRADE_PROVINCE_ORDER_WINDOW = "dws_trade_province_order_window";
    public static final String DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW = "dws_trade_trademark_category_user_refund_window";



    public static final String DIM_APP = "dim_app";

    public static final int TWO_DAY_SECONDS = 4;
}

