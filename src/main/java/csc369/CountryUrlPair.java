package csc369;

import java.io.*;
import org.apache.hadoop.io.*;

public class CountryUrlPair implements Writable, WritableComparable<CountryUrlPair>{
    private Text country = new Text();
    private Text Url = new Text();

    public Text getCountry() {
        return country;
    }

    public Text getUrl() {
        return Url;
    }

    public CountryUrlPair(){
    }
    
    public CountryUrlPair(String country, String Url) {
        this.country.set(country);
        this.Url.set(Url);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        Url.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        country.write(out);
        Url.write(out);
    }

    @Override
    public int compareTo(CountryUrlPair pair) {
        if (country.compareTo(pair.getCountry()) == 0) {
            return Url.compareTo(pair.getUrl());
        }
        return country.compareTo(pair.getCountry());
    }

    @Override
    public String toString() {
        return this.country.toString() + " " + this.Url.toString();
    }
}
