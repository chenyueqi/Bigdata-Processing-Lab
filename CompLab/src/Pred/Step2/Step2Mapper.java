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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

public class Step2Mapper extends Mapper<Object, Text, Text, LongWritable>
{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

	FileSplit fileSplit = (FileSplit)context.getInputSplit();
	String fileName = fileSplit.getPath().getName();

	if(fileName.equals("_SUCCESS"))
	    return;

	String urlHour = value.toString().split("\t")[0];
	String fre = value.toString().split("\t")[1];

	String url = urlHour.split("#")[0];
	String hour = urlHour.split("#")[1];


	long out  = 0;
	if(fre.contains("#"))
	{
	    long val1 = Long.parseLong(fre.split("#")[0]);
	    long val2 = Long.parseLong(fre.split("#")[1]);
	    out = (val1 - val2) * (val1 - val2);
	}
	else
	{
	    long val1 = Long.parseLong(fre);
	    out = val1 * val1;
	}

	context.write(new Text(hour + "#" +url),  new LongWritable(out));

    }
}
