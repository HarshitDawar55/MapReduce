import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CounterDriver {
	public static void main(String args[]) throws Exception {
		 /*if(args.length!=2){
		 System.exit(-1);
		 }*/
		Configuration c = new Configuration();
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		// @SuppressWarnings("deprecation")
		Job j = Job.getInstance(c);
				//Job.getInstance(c);// new Job(c, "Counter for Images");
		j.setJarByClass(CounterDriver.class);
		FileInputFormat.setInputPaths(j, input);
		FileOutputFormat.setOutputPath(j, output);
		j.setMapperClass(CounterMapper.class);
		// j.setReducerClass(ToolRunnerReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(DoubleWritable.class);
		j.setNumReduceTasks(0);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
		//boolean s = j.waitForCompletion(true);
		//if(s){
			long gif = j.getCounters().findCounter("ImageCounter", "gif")
					.getValue();

			long jpg = j.getCounters().findCounter("ImageCounter", "jpg")
					.getValue();

			long other = j.getCounters().findCounter("ImageCounter", "other")
					.getValue();

			System.out.println("Number of gif's = " + gif + "\n"
					+ "Number of jpg's = " + jpg + "\n" + "Number of other = "
					+ other);
		//	return 0;
		//}
		//else{
			//return 1;
		//}
		
		

	}

}
