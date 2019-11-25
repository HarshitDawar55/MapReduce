import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		int wordOccurence = 0;
		
		for (DoubleWritable value : values) {
			wordOccurence += value.get();
		}
		context.write(key, new DoubleWritable(wordOccurence));
	}

}
