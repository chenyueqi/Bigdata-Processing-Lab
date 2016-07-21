/*
 * Map class for Step1 
 * Project Name: Comprehensive Lab
 * Group Name: What the f**k
 * Created: Yueqi Chen (Yueqichen.0x0@gmail.com)
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

	if(logs.length < 5)
	    return ;

    	String interfaces = logs[4]; // URL
    	String time = logs[1]; //timestamp in grain of hour
    	
    	String[] times = time.split(":");
	String temp = times[0].split("/")[0];
	String dateIn = temp.substring(1, temp.length());// the date of record

	FileSplit fileSplit = (FileSplit) context.getInputSplit();
	String fileName = fileSplit.getPath().getName();
	temp = fileName.split(".log")[0];
	String dateOut = temp.split("-")[2]; // the date of file

	if(dateOut.equals(dateIn)  && !interfaces.equals("null")) //check if date of file = date of record
	    context.write(new Text(interfaces + "#" + times[1] + "#" + dateIn),new IntWritable(1));
    }
}
