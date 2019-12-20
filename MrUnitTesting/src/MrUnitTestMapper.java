
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MrUnitTestMapper extends
		Mapper<DoubleWritable, Text, Text, DoubleWritable> {
	
	public void map(DoubleWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		for (String word : line.split("\\W+")) {

			if (word.length() > 0) {
				context.write(new Text(word),
						new DoubleWritable(1));
			}
		}
	}

}

