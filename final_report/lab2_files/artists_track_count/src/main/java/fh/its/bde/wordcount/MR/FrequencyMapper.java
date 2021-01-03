package fh.its.bde.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencyMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private final static IntWritable one = new IntWritable(1);

  @Override
  public void map(LongWritable offset, Text lineText, Context context)
      throws IOException, InterruptedException {

    String line = lineText.toString();
    String artists = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[1];
    artists = artists.replaceAll("\"", "");
    if (artists.length() < 2) return;
    artists = artists.substring(1,artists.length()-1);
    String[] artistsArr = artists.split(",");

    for (String artist : artistsArr) {
      if (artist.length() < 2) continue;
      String artistSub=artist.trim();
      artistSub = artistSub.replaceAll("'", "");
      context.write(new Text(artistSub.trim()), one);
    }
       
        
  }
}
