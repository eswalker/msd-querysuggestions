/**
 * 
 */
package com.eswalker.msd.QuerySuggestions;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.eswalker.msd.QuerySuggestions.QuerySuggestions.HMapper;
import com.eswalker.msd.QuerySuggestions.QuerySuggestions.HReducer;


 
public class QuerySuggestionsTest {
 
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;
 
  @Before
  public void setUp() throws ParseException {

  
    HMapper mapper = new HMapper();
    HReducer reducer = new HReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  
  }
 
  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "1|TRAID|artist|title|1|hel lo,100"
    ));

    mapDriver.withOutput(new Text("h"), new Text("hel lo"));
    mapDriver.withOutput(new Text("he"), new Text("hel lo"));
    mapDriver.withOutput(new Text("hel"), new Text("hel lo"));
    mapDriver.withOutput(new Text("l"), new Text("hel lo"));
    mapDriver.withOutput(new Text("lo"), new Text("hel lo"));
    mapDriver.runTest();
  }
  
 
	  
  
  @Test
  public void testReducer() throws IOException {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("hello"));
    values.add(new Text("hero"));
    values.add(new Text("hero"));
    reduceDriver.withInput(new Text("he"), values );
    reduceDriver.withOutput(NullWritable.get(), new Text("he|hero|hello"));
    reduceDriver.runTest();
  }
  
 
}