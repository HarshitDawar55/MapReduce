import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CounterDriver1  extends Configured implements Tool {
	
	public int run(String args[]) throws Exception{
		//@SuppressWarnings("deprecation")
		Path input = new Path(args[0]);
		Path output= new Path(args[1]);
		@SuppressWarnings("deprecation")
		Job j = new Job(getConf(), "Average Length Calculator Class");
		j.setJarByClass(CounterDriver1.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		j.setMapperClass(CounterMapper.class);
		//j.setReducerClass(ToolRunnerReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(DoubleWritable.class);
		j.setNumReduceTasks(0);
		boolean success = j.waitForCompletion(true);
		if(success){

			long gif = j.getCounters().findCounter("ImageCounter", "gif")
					.getValue();

			long jpg = j.getCounters().findCounter("ImageCounter", "jpg")
					.getValue();

			long other = j.getCounters().findCounter("ImageCounter", "other")
					.getValue();

			System.out.println("Number of gif's = " + gif + "\n"
					+ "Number of jpg's = " + jpg + "\n" + "Number of other = "
					+ other);
			return 0;
		}
		else{
			return 1;
		}
	}
	public static void main(String args[]) throws Exception {
		// if(args.length!=2){
		// System.exit(-1);
		// }
		Configuration c = new Configuration();
		

		//System.exit(j.waitForCompletion(true) ? 0 : 1);
		int exitcode = ToolRunner.run( c, new CounterDriver1(), args);
		System.exit(exitcode);
	

	}
	

}
