package hadoop2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SequenceFileOutputFormatHelper {
    public void setOutputPath(Job job,
            Path outputDir) {
        SequenceFileOutputFormat.setOutputPath(job, outputDir);
    }

}
