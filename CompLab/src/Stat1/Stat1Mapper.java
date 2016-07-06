/*
 * Map class for Stat1
 * Project Name: 
 * Group Name: What the f**k
 * Created: Wei Liu (lw_nju@outlook.com)
 * Time: 2016/7/5 15:50
*/


import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

public class Stat1Mapper extends Mapper<Object, Text, Text, IntWritable>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
    	String[] logs = value.toString().split(" ");

    	String state = logs[7];
    	String time = logs[1];
    	
    	String[] times = time.split(":");
    	
    	
    	context.write(new Text("0#"+state),new IntWritable(1));
    	context.write(new Text("1#"+times[1]+":00@"+state),new IntWritable(1));
    }
}
