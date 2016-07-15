/*
 * Map class for Stat3 - URL
 * Project Name: Comprehensive Lab
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

public class Stat3Mapper extends Mapper<Object, Text, Text, IntWritable>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
    	String[] logs = value.toString().split(" ");

    	String interfaces = logs[4]; // URL
    	String time = logs[1]; //timestamp in grain of second
    	
    	String[] times = time.split(":");
    	context.write(new Text("0#"+interfaces),new IntWritable(1));
    	context.write(new Text("1#"+times[1]+":"+times[2]+":"+times[3]+"@"+interfaces),new IntWritable(1));
    }
}
