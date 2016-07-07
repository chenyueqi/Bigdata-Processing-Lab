import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public static class MultipleOutput extends
        MultipleOutputFormat<Text, IntWritable>
{
    private TextOutputFormat<Text, IntWritable> output = null;
    @Override
    protected RecordWriter<Text, IntWritable> getBaseRecordWriter(
            FileSystem fs, JobConf job, String name, Progressable arg3)
            throws IOException
    {
        if (output == null) {
            output = new TextOutputFormat<Text, IntWritable>();
        }
        return output.getRecordWriter(fs, job, name, arg3);
    }
    @Override
    protected String generateFileNameForKeyValue(Text key,
                                                 IntWritable value, String name)
    {
        String keystr = key.toString();
        if(keystr.contains("@"))
            return keystr.split("#")[1].split("@")[1]+".txt";
        return keystr.split("#")[1]+".txt";
    }
}