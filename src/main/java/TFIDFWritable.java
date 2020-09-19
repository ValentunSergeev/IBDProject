import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TFIDFWritable implements Writable {
    private List<Pair<Integer, Double>> tfidf;

    public TFIDFWritable(List<Pair<Integer, Double>> tfidf) {
        this.tfidf = tfidf;
    }

    public List<Pair<Integer, Double>> getTfidf() {
        return tfidf;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        var key = new IntWritable();
        var value = new DoubleWritable();

        var length = new IntWritable(tfidf.size());

        length.write(out);

        for (var pair : tfidf) {
            key.set(pair.getKey());
            value.set(pair.getValue());

            out.writeChar('(');

            key.write(out);

            out.writeChar(':');

            value.write(out);

            out.writeChars("),");
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        var key = new IntWritable();
        var value = new DoubleWritable();

        var length = new IntWritable();

        length.readFields(in);

        var list = new ArrayList<Pair<Integer, Double>>(length.get());

        for (int i = 0; i < length.get(); i++) {
            in.readChar();

            key.readFields(in);

            in.readChar();

            value.readFields(in);

            in.readChar();
            in.readChar();

            list.add(new Pair<>(Utils.hashOf(key.toString()), value.get()));
        }

        tfidf = list;
    }
}
