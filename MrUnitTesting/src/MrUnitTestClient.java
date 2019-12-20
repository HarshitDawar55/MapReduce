import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class MrUnitTestClient {

	/*
	 * Declare harnesses that let you test a mapper, a reducer, and a mapper and
	 * a reducer working together.
	 */

	MapDriver<DoubleWritable, Text, Text, DoubleWritable> mapDriver;
	ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<DoubleWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {
		/* Set up mapper test harness. */

		Mapper<DoubleWritable, Text, Text, DoubleWritable> mapper = new MrUnitTestMapper();
		mapDriver = new MapDriver<DoubleWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		/*
		 * Set up the reducer test harness.
		 */
		MrUnitTestReducer reducer = new MrUnitTestReducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<DoubleWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	  /*
	   * Test the mapper.
	   */
	  @Test
	  public void testMapper(){
	   mapDriver.withInput(new DoubleWritable(1),new Text("cat cat dog"));
	   mapDriver.addOutput(new Text("cat"), new DoubleWritable(1));
	   mapDriver.addOutput(new Text("cat"), new DoubleWritable(1));
	   mapDriver.addOutput(new Text("dog"), new DoubleWritable(1));
	   try {
	        mapDriver.runTest();
	} catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	}
	    /*
	     * For this test, the mapper's input will be "1 cat cat dog"
	     * TODO: implement
	     */

	          //fail("Please implement test.");

	  }

	  /*
	   * Test the reducer.
	   */
	  @Test
	  public void testReducer() throws IOException {

	    /*
	     * For this test, the reducer's input will be "cat 1 1".
	     * The expected output is "cat 2".
	     * TODO: implement
	     */
	         List<DoubleWritable> resultValues = new ArrayList<DoubleWritable>();
	         resultValues.add(new DoubleWritable(1));
	         resultValues.add(new DoubleWritable(1));
	         reduceDriver.withInput(new Text("cat"), resultValues);
	         reduceDriver.addOutput(new Text("cat"), new DoubleWritable(2));
	         reduceDriver.runTest();

	          //fail("Please implement test.");
	  }

	  /*
	   * Test the mapper and reducer working together.
	   */
	  @Test
	  public void testMapReduce() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog"
	     * The expected output (from the reducer) is "cat 2", "dog 1".
	     * TODO: implement
	     */
	          mapReduceDriver.withInput(new DoubleWritable(1), new Text("cat cat dog"));
	          mapReduceDriver.addOutput(new Text("cat"), new DoubleWritable(2));
	          mapReduceDriver.addOutput(new Text("dog"), new DoubleWritable(1));

	  }

}
