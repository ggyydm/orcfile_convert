package domob.inf.hive;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;

/**
 * Created by domob on 2017/1/9.
 */
public class FileFormatChange {
    public String hadoopConfigDir;
    public Configuration conf;
    public FileHandler inputHandler;
    public FileHandler outputHandler;
    public  class Params {
        @Parameter(
                description = "input file path",
                names = {"-i"}
        )
        String input;
        @Parameter(
                description = "output file path",
                names = {"-o"}
        )
        String output;
        @Parameter(
                description = "RECORD_INTERVAL_CONF_STR",
                names = {"-c"}
        )
        String record_interval;
        @Parameter(
                description = "HADOOP_CONF_DIR",
                names = {"--hadoopconfig"}
        )
        String hadoop_conf="/usr/local/domob/current/hadoop/conf";
        @Parameter(
                description = "HADOOP_CONF_DIR",
                names = {"--hiveconfig"}
        )
        String hive_conf="/usr/local/domob/current/hive/conf";
        @Parameter(
                description = "input file format",
                names = {"--inputFileFormat"}
        )
        String iFileFormat;
        @Parameter(
                description = "output file format",
                names = {"--outputFileFormat"}
        )
        String oFileFormat;
        @Parameter(
                description = "hive table info, should like db.table",
                names = {"--table"}
        )
        String Hivetable;

        @Parameter(
                description = "separator of the row",
                names = {"-s"}
        )
        String separator="\t";


    }
    public boolean init(String[] args){
        final Params params = new Params();
        JCommander commander = new JCommander(params, args);
        conf= new Configuration(false);
        conf.addResource(new Path(hadoopConfigDir+"/core-site.xml"));
        conf.addResource(new Path(hadoopConfigDir+"/hdfs-site.xml"));
        String iFileformat=params.iFileFormat;
        String oFileformat=params.oFileFormat;
        boolean result = true;
        if(iFileformat.equals("txtfile")){
            inputHandler = new TXTFileHandler();
            result = inputHandler.initReader(params);
        }
        else if(iFileformat.equals("orcfile")){
            inputHandler = new ORCFileHandler();
            result = inputHandler.initReader(params);
        }
        else if(iFileformat.equals("rcfile")){
            inputHandler = new RCFileHandler();
            result = inputHandler.initReader(params);
        }
        else{
            System.err.println("input handler init failed");
            return false;
        }
        if (!result){
            return false;
        }
        if(oFileformat.equals("orcfile")){
            outputHandler = new ORCFileHandler();
            result = outputHandler.initWriter(params);
        }
        else if(oFileformat.equals("txtfile")){
            outputHandler = new TXTFileHandler();
            result = outputHandler.initWriter(params);
        }
        else if(oFileformat.equals("rcfile")){
            outputHandler = new RCFileHandler();
            result = outputHandler.initWriter(params);
        }
        else{
            System.err.println("output handler init failed");
            return false;
        }
        return result;
    }
    public int run(String[] args){
        boolean result = true;
        try {
            if(!init(args)){
                return 1;
            }
            String row;

            while(!inputHandler.readFinish() && result){
                row = inputHandler.readOneRow();
                if(row!=null) {
                    result = outputHandler.writeRecords(row);
                }
            }
            outputHandler.writeFinish();

        }catch (Exception e){
            e.printStackTrace();
            return 1;
        }
        if(!result){
            return 1;
        }
        return  0;
    }

    public static void main(String[] args){
        FileFormatChange change = new FileFormatChange();

        System.exit(change.run(args));

    }

}
