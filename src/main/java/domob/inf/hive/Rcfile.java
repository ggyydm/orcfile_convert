package domob.inf.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.util.GenericOptionsParser;
//import com.google.common.primitives.Bytes;
//import com.sun.tools.javah.Util;
import org.apache.hadoop.io.Text;

import java.io.EOFException;
import java.io.IOException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
/**
 * Created by domob on 2016/12/2.
 */

public class Rcfile {
    private  Configuration conf;
    private  int colNum=-1;
    private  boolean recoverFlag=false;
    private Path o_path;
    private  class Params {
        @Parameter(
                description = "input path",
                names = {"-i"}
        )
        String input;
    }
    public int run(String[] args) throws Exception {


        final Params params = new Params();
        JCommander commander = new JCommander(params, args);
        conf= new Configuration();
        int result;

        if(params.input==null){
            commander.usage();
            return 1;
        }
        Path i_path = new Path(params.input);
        result = readRcFile(i_path, conf);



        return result;
    }
    private  int readRcFile(Path src, Configuration conf)
            throws IOException {

        ColumnProjectionUtils.setReadAllColumns(conf);
        FileSystem fs = FileSystem.get(conf);
        RCFile.Reader reader = new RCFile.Reader(fs, src, conf);

        int result = readerByRow(reader, recoverFlag);

        reader.close();

        fs.close();
        return  result;
    }

    protected  int readerByRow(RCFile.Reader reader, boolean recoverFlag) throws IOException {
        // 已经读取的行数
        LongWritable rowID = new LongWritable();
        // 一个行组的数据
        BytesRefArrayWritable cols = new BytesRefArrayWritable();
        RCFile.Writer writer= null;
        FileSystem fs;

        while (reader.next(rowID)) {
            //System.out.println("rowId: "+rowID.toString());
            StringBuilder sb = new StringBuilder();
            try {
                reader.getCurrentRow(cols);
                // 包含一列的数据
                BytesRefWritable brw = null;

                Text txt = new Text();
                if (colNum < 0) {
                    colNum = cols.size();
                    System.out.println("file has " + cols.size() + " colunms");
                }
                if (recoverFlag == false) {
                    for (int i = 0; i < cols.size(); i++) {
                        brw = cols.get(i);
                        // 根据start 和 length 获取指定行-列数据
                        txt.set(brw.getData(), brw.getStart(), brw.getLength());
                        sb.append(txt.toString());
                        if (i < cols.size() - 1) {
                            sb.append("\t");
                        }
                    }

                }
            }catch (EOFException e){
                    if (writer != null) {
                        System.out.println("handle meet EOF, already recover " + rowID + " records.");
                        return 0;
                    }
                    else{
                        System.out.println("RCFile is bad, " + rowID + " records can be recoverd.");
                        return  1;
                    }
                }
                System.out.println(sb.toString());
            }

            return  0;
        }

    public static  void main(String[] args) {
        Rcfile rcFile = new Rcfile();
        try {
            int result = rcFile.run(args);

            System.exit(result);
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }

    }
}


