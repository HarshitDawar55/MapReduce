import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LogClient {
		public static void main(String args[]) throws Exception{
			if(args.length!=2){
				System.exit(-1);
			}
			
				Configuration c = new Configuration();
				@SuppressWarnings("deprecation")
				Job j = new Job(c, "Log Calculator Class");
				Path input = new Path(args[0]);
				Path output= new Path(args[1]);
				j.setJarByClass(LogClient.class);
				j.setMapperClass(LogMapper.class);
				j.setReducerClass(LogReducer.class);
				j.setOutputKeyClass(Text.class);
				j.setOutputValueClass(DoubleWritable.class);
				FileInputFormat.addInputPath(j, input);
				FileOutputFormat.setOutputPath(j, output);
				System.exit(j.waitForCompletion(true)?0:1);
			
		}
		
	
}
