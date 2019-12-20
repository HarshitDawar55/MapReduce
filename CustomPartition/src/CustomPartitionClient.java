


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CustomPartitionClient {
		public static void main(String args[]) throws Exception{
			if(args.length!=2){
				System.exit(-1);
			}
			
				Configuration c = new Configuration();
				Job j = Job.getInstance(c);
				j.setJobName("Custom Partition Program");
				Path input = new Path(args[0]);
				Path output= new Path(args[1]);
				j.setJarByClass(CustomPartitionClient.class);
				j.setMapperClass(WordCountMapper.class);
				j.setReducerClass(WordCountReducer.class);
				j.setOutputKeyClass(Text.class);
				j.setOutputValueClass(DoubleWritable.class);
				FileInputFormat.addInputPath(j, input);
				FileOutputFormat.setOutputPath(j, output);
				j.setNumReduceTasks(27);
				j.setPartitionerClass(CustomPartition.class);
				System.exit(j.waitForCompletion(true)?0:1);
			
		}
		
	
}
