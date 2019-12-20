import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		int wordOccurence = 0;
		//double totallength = 0;
		//double average = 0;
		for (DoubleWritable value : values) {
			//totallength += value.get();
			wordOccurence += value.get();
		}
		//average = totallength / wordOccurence;
		context.write(key, new DoubleWritable(wordOccurence));
	}

}
