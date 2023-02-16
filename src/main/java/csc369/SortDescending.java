package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class SortDescending {
    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
	    context.write(new LongWritable(Long.parseLong(sa[1])), new Text(sa[0]));
        }
    }

    // used to perform secondary sort on temperature
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(LongWritable.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            LongWritable count1 = (LongWritable) wc1;
            LongWritable count2 = (LongWritable) wc2;
            return -1 * count1.compareTo(count2);
        }
    }

    public static class ReducerImpl extends Reducer<LongWritable, Text, Text, LongWritable> {
        @Override
	protected void reduce(LongWritable count, Iterable<Text> hostnames,
			      Context context) throws IOException, InterruptedException {
            
            Iterator<Text> itr = hostnames.iterator();
            while (itr.hasNext()){
                context.write(itr.next(), count);
            }
        }
    }
}
