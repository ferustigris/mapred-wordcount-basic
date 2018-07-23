import com.epam.mapreduce.WordCountMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class WordCountTest  {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void wordCountTest() throws IOException {
        mapDriver.withInput(new LongWritable(2), new Text("asd asd"));
        mapDriver.withOutput(new Text("asd"), new LongWritable(1));
        mapDriver.withOutput(new Text("asd"), new LongWritable(1));
        mapDriver.runTest();
    }
}
