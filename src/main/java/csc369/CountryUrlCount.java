package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryUrlCount {
    public static final Class OUTPUT_KEY_CLASS = CountryUrlPair.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryUrlPair, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
	    String country = sa[0];
        sa = sa[1].split(" ");
	    context.write(new CountryUrlPair(country, sa[6]), one);
        }
    }

    public static class ReducerImpl extends Reducer<CountryUrlPair, IntWritable, CountryUrlPair, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(CountryUrlPair pair, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(pair, result);
       }
    }
}
