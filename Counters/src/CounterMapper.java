import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		int dot = line.indexOf(".", line.indexOf("GET "));
		int end = line.indexOf(" ",dot);
		String required = line.substring(dot,end).trim();
		if(required.endsWith("gif")){
			context.getCounter("ImageCounter","gif").increment(1);
		}
		else if(required.endsWith("jpg")){
			context.getCounter("ImageCounter","jpg").increment(1);
		}
		else{
			context.getCounter("ImageCounter","other").increment(1);
		}
	}

}
