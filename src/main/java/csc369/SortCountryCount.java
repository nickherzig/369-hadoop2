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

public class SortCountryCount {
    public static final Class OUTPUT_KEY_CLASS = CountryCountPair.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryCountPair, Text> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
        

        String country = "";
	    String[] pa = sa[0].split(" ");
        for(int i = 0; i < pa.length - 1; i++){
            country += pa[i] + " ";
        }

        context.write(new CountryCountPair(country, Integer.parseInt(sa[1])), new Text(pa[pa.length-1]));
	    
        }
    }

    // used to perform secondary sort on temperature
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(CountryCountPair.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            CountryCountPair pair1 = (CountryCountPair) wc1;
            CountryCountPair pair2 = (CountryCountPair) wc2;
            return pair1.compareTo(pair2);
        }
    }

    public static class ReducerImpl extends Reducer<CountryCountPair, Text, Text, Text> {
        @Override
	protected void reduce(CountryCountPair pair, Iterable<Text> urls,
			      Context context) throws IOException, InterruptedException {
            
            Iterator<Text> itr = urls.iterator();
            while (itr.hasNext()){
                context.write(pair.getCountry(), new Text(itr.next().toString() + " " + pair.getCount().toString()));
            }
        }
    }
}
