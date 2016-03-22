import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;


public class WikiPageRanking {
public static void main(String[] args) throws IOException {
 
	
      JobConf conf = new JobConf(WikiPageRanking.class);
 
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        // Mahout class to Parse XML + config
        conf.setInputFormat(XmlInputFormat.class);
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        // Our class to parse links from content.
        conf.setMapperClass(WikiPageLinksMapper.class);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        // Our class to create initial output
        conf.setReducerClass(WikiLinksReducer.class);
 
        JobClient.runJob(conf);
 
    }
}
