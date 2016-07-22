/*
 * Map class for Step1 
 * Project Name: Comprehensive Lab
 * Group Name: What the f**k
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

public class Step1Mapper extends Mapper<Object, Text, Text, Text>
{
    static int[] In= new int[15];
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

        for(int i=0;i<15;i++)
            In[i] = 0;
        In[Integer.parseInt(dateOut)-8] = 1;

	    if(dateOut.equals(dateIn)  && !interfaces.equals("null")) //check if date of file = date of record
            context.write(new Text(interfaces + "#" + times[1]),new Text(
                        In[0] + " " +
                        In[1] + " " +
                        In[2] + " " +
                        In[3] + " " +
                        In[4] + " " +
                        In[5] + " " +
                        In[6] + " " +
                        In[7] + " " +
                        In[8] + " " +
                        In[9] + " " +
                        In[10] + " " +
                        In[11] + " " +
                        In[12] + " " +
                        In[13] + " " + In[14]
                        ));
    }
}
