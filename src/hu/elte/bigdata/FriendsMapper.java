package hu.elte.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendsMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    // TODO Miért így láttam minden példában? Ez így optimális? A context rögtön kiírja, így mindegy a példány?
    private final IntWritable userId = new IntWritable(-1);
    private final IntWritable friendId = new IntWritable(-1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        userId.set(Integer.parseInt(fields[0]));
        String[] friends = fields[1].split(",");
        for (String friend : friends) {
            friendId.set(Integer.parseInt(friend));
            context.write(userId, friendId);
        }
    }
}
