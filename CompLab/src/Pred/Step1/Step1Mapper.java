/*
 * Map class for Step1 
 * Project Name: Comprehensive Lab
 * Group Name: What the f**k
 * Created: Wei Liu (lw_nju@outlook.com)
 * Time: 2016/7/5 15:50
 * descripe: to get all the 
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

public class Step1Mapper extends Mapper<Object, Text, Text, IntWritable>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
    	String[] logs = value.toString().split(" ");

    	String interfaces = logs[4]; // URL
    	String time = logs[1]; //timestamp in grain of hour
    	
    	String[] times = time.split(":");
	String dateIn = times[0].split("/")[0]; // the date of record

	FileSplit fileSplit = (FileSplit) context.getInputSplit();
	String fileName = fileSplit.getPath().getName();
	String dateOut = fileName.split(".")[0].split("-")[2]; // the date of file

	if(dateOut.equals(dateIn) && !dateOut.equals("22")) //check if date of file = date of record
	{
	    context.write(new Text(interfaces + "#" + times[1] + "#" + dateIn),new IntWritable(1));
	}
    }
}