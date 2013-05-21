package de.uni_potsdam.hpi.loddp.pigudf;

import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class QuadLoader extends FileInputLoadFunc {
    private String location;
    @Override
    public void setLocation(String location, org.apache.hadoop.mapreduce.Job job) throws IOException {
        this.location = location;
    }

    @Override
    public org.apache.hadoop.mapreduce.InputFormat getInputFormat() throws IOException {
        return null;
    }

    @Override
    public void prepareToRead(org.apache.hadoop.mapreduce.RecordReader reader, PigSplit split) throws IOException {
    }

    @Override
    public Tuple getNext() throws IOException {
        return null;
    }
}
