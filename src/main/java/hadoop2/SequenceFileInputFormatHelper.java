package hadoop2;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class SequenceFileInputFormatHelper {
    public void setInputPaths(Job job,
            Path path) throws IOException {
        SequenceFileInputFormat.setInputPaths(job, path);
    }
}
