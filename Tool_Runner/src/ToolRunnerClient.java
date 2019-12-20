


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ToolRunnerClient extends Configured implements Tool{
		public static void main(String args[]) throws Exception{
			if(args.length!=2){
				System.exit(-1);
			}
			
				//Configuration c = new Configuration();
				int exitcode = ToolRunner.run( new Configuration(), new ToolRunnerClient(), args);
				System.exit(exitcode);
			
		}
		
		public int run(String args[]) throws Exception{
			//@SuppressWarnings("deprecation")
			Path input = new Path(args[0]);
			Path output= new Path(args[1]);
			@SuppressWarnings("deprecation")
			Job j = new Job(getConf(), "Average Length Calculator Class");
			j.setJarByClass(ToolRunnerClient.class);
			FileInputFormat.addInputPath(j, input);
			FileOutputFormat.setOutputPath(j, output);
			j.setMapperClass(ToolRunnerMapper.class);
			j.setReducerClass(ToolRunnerReducer.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(DoubleWritable.class);
			return(j.waitForCompletion(true)?0:1);
		}
		
	
}
