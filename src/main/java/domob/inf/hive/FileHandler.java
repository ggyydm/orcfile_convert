package domob.inf.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;

/**
 * Created by domob on 2017/1/9.
 */
public interface FileHandler {
    public String readOneRow();
    public boolean initReader(FileFormatChange.Params args);
    public boolean readFinish();
    public boolean initWriter(/*String Path,Configuration conf*/FileFormatChange.Params args);
    public boolean writeRecords(String row);
    public boolean writeFinish();
}
