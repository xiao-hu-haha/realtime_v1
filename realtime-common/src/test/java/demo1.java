import com.bw.gmall.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class demo1 extends BaseApp{


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) {

    }

    public static void main(String[] args) {
        demo1 dem = new demo1();
        dem.start("topic_db","topic_db",4,2020);
    }

}
