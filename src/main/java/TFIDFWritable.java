import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TFIDFWritable implements Writable {
    private List<Pair<Long, Double>> tfidf;

    public TFIDFWritable(List<Pair<Long, Double>> tfidf) {
        this.tfidf = tfidf;
    }

    public List<Pair<Long, Double>> getTfidf() {
        return tfidf;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        String length = String.valueOf(tfidf.size());

        builder.append(length);

        for (Pair<Long, Double> pair : tfidf) {
            builder.append('(').append(pair.getKey()).append(':')
                    .append(pair.getValue()).append("),");
        }

        return builder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        LongWritable key = new LongWritable();
        DoubleWritable value = new DoubleWritable();

        IntWritable length = new IntWritable(tfidf.size());

        length.write(out);

        for (Pair<Long, Double> pair : tfidf) {
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
        LongWritable key = new LongWritable();
        DoubleWritable value = new DoubleWritable();

        IntWritable length = new IntWritable();

        length.readFields(in);

        ArrayList<Pair<Long, Double>> list = new ArrayList<>(length.get());

        for (int i = 0; i < length.get(); i++) {
            in.readChar();

            key.readFields(in);

            in.readChar();

            value.readFields(in);

            in.readChar();
            in.readChar();

            list.add(new Pair<>(key.get(), value.get()));
        }

        tfidf = list;
    }
}
