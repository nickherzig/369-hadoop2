package csc369;

import java.io.*;
import org.apache.hadoop.io.*;

public class CountryCountPair implements Writable, WritableComparable<CountryCountPair> {
    private Text country = new Text();
    private IntWritable count = new IntWritable();

    public Text getCountry() {
        return country;
    }

    public IntWritable getCount() {
        return count;
    }

    public CountryCountPair(){
        
    }
    
    public CountryCountPair(String country, int count) {
        this.country.set(country);
        this.count.set(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        count.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        country.write(out);
        count.write(out);
    }

    @Override
    public int compareTo(CountryCountPair pair) {
        if (country.compareTo(pair.getCountry()) == 0) {
            return -count.compareTo(pair.getCount());
        }
        return country.compareTo(pair.getCountry());
    }

}