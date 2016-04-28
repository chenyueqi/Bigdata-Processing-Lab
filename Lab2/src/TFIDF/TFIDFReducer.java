/*
 * Reducer class for InvertedIndex Table
 * Project Name: Inverted Index Table for Hadoop (Lab2)
 * Group Name: What the f**k
 * Created by: Yueqi Chen (Yueqichen.0x0@gmail.com)
 * Time: 2016/4/28 19:00
*/

import java.io.IOException;
import java.util.*;
import java.lang.Math.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;

public class TFIDFReducer extends Reducer<Text, IntWritable, Text, Text>
{
    static List<String> postingList = new ArrayList<String>();
    static Text CurrentItem = new Text(" ");

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
	int sum = 0;
	Text word1 = new Text();
	Text word2 = new Text();

	String[] wordauthorfile = key.toString().split("#");

	word1.set(wordauthorfile[0] + "#" + wordauthorfile[1]);//word+author

	String filename = new String();
	filename = wordauthorfile[2];//filename

	for(IntWritable val : values)
	    sum += val.get();

	word2.set(filename + ":" + sum);


	if(!CurrentItem.equals(word1) && (!CurrentItem.equals(" ")))
	{
	    StringBuffer out = new StringBuffer();
	    long cnt_word = 0;
	    long cnt_file = 0;
	    for(String p:postingList)
	    {
		long num =  Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
		if(num >0)
		    cnt_file++;
		cnt_word += num;
	    }

	    if(cnt_file > 0 && CurrentItem.toString().split("#").length == 2)
	    {
		String author = CurrentItem.toString().split("#")[1];
		String word = CurrentItem.toString().split("#")[0];
		out.append(word + ", ");
		out.append(cnt_word + ", ");
		if(author.equals("卧龙生"))
		    out.append(Math.log((double)54/(double)(cnt_file + 1)));
		else if(author.equals("古龙"))
		    out.append(Math.log((double)70/(double)(cnt_file + 1)));
		else if(author.equals("李凉"))
		    out.append(Math.log((double)41/(double)(cnt_file + 1)));
		else if(author.equals("梁羽生"))
		    out.append(Math.log((double)38/(double)(cnt_file + 1)));
		else if(author.equals("金庸"))
		    out.append(Math.log((double)15/(double)(cnt_file + 1)));
	    }

	    if(cnt_word > 0 && CurrentItem.toString().split("#").length == 2)
		context.write(new Text(CurrentItem.toString().split("#")[1]), new Text(out.toString()));
	    postingList = new ArrayList<String>();
	}
	CurrentItem = new Text(word1);
	postingList.add(word2.toString());
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
	StringBuffer out = new StringBuffer();

	long cnt_word = 0;
	long cnt_file = 0;
	for(String p:postingList)
	{
	    long num =  Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
	    if(num >0)
		cnt_file++;
	    cnt_word += num;
	}

	if(cnt_file > 0 && CurrentItem.toString().split("#").length == 2)
	{
	    String author = CurrentItem.toString().split("#")[1];
	    String word = CurrentItem.toString().split("#")[0];
	    out.append(word + ", ");
	    out.append(cnt_word + ", ");
	    if(author.equals("卧龙生"))
		out.append(Math.log((double)54/(double)(cnt_file + 1)));
	    else if(author.equals("古龙"))
		out.append(Math.log((double)70/(double)(cnt_file + 1)));
	    else if(author.equals("李凉"))
		out.append(Math.log((double)41/(double)(cnt_file + 1)));
	    else if(author.equals("梁羽生"))
		out.append(Math.log((double)38/(double)(cnt_file + 1)));
	    else if(author.equals("金庸"))
		out.append(Math.log((double)15/(double)(cnt_file + 1)));
	}

	if(cnt_word > 0 && CurrentItem.toString().split("#").length == 2)
	    context.write(new Text(CurrentItem.toString().split("#")[1]), new Text(out.toString()));
    }
}
