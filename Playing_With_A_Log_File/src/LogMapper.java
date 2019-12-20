
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		//for (String word : line.split("\\W+")) {

			//if (word.length() > 0) {
				//context.write(new Text(word),
			//			new DoubleWritable(1));
			//}
		//}
		String ip = line.substring(0, line.indexOf(" ")).trim();
		if(ip.length()>0){
			context.write(new Text(ip),new DoubleWritable(1));
		}
	}

}

