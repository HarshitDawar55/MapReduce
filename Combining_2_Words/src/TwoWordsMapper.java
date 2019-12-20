
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwoWordsMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringBuffer lastword = new StringBuffer();
		boolean firstIteration = true;
		for (String word : line.split("\\W+")) {

			if (word.length() > 0) {
				if(firstIteration){
					lastword.append(word.toUpperCase());
					firstIteration = false;
					continue;
				}
				lastword.append(',');
				lastword.append(word.toUpperCase());
				context.write(new Text(lastword.toString()),
						new DoubleWritable(1));
				lastword.delete(0, lastword.length());
				lastword.append(word.toLowerCase());
			}
		}
	}

}

