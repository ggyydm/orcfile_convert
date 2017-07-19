package domob.inf.hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.RCFile;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
/**
 * Created by domob on 2016/12/2.
 */

public class RCFileHandle {
    private  Configuration conf;
    private  int colNum=-1;
    private  boolean recoverFlag=false;
    private Path o_path;
    int record_interval;
    private String hadoopConfigDir;
    private  class Params {
        @Parameter(
                description = "input path",
                names = {"-i"}
        )
        String input;
        @Parameter(
                description = "output path",
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
                names = {"--config"}
        )
        String hadoop_conf;

    }
    public int run(String[] args) throws Exception {


        final Params params = new Params();
        JCommander commander = new JCommander(params, args);
        if(params.hadoop_conf!=null){
            hadoopConfigDir=params.hadoop_conf;

        }
        else{
            hadoopConfigDir="/usr/local/domob/current/hadoop/conf";
        }

        conf= new Configuration(false);
        conf.addResource(new Path(hadoopConfigDir+"/core-site.xml"));
        conf.addResource(new Path(hadoopConfigDir+"/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        //System.out.print(conf.toString());
        int result;

        if(params.input==null){
            commander.usage();
            return 1;
        }
        String input = params.input;
        String hdfsMark="hdfs://";
        String fileMark="file://";
        if(!input.contains(hdfsMark)&&!input.contains(fileMark)) {
            if (input.startsWith("/")) {
                input = fileMark + input;
            } else {
                File file = new File(input);
                input = fileMark + file.getAbsolutePath();

            }
        }
        Path i_path = new Path(input);
        if(params.output!=null){
            recoverFlag=true;
            o_path = new Path(params.output);

        }
        if(params.record_interval==null){
            record_interval=100;
        }
        else{
            record_interval=Integer.parseInt(params.record_interval);
        }

        result = readRcFile(i_path, conf);



        return result;
    }
        private  int readRcFile(Path src, Configuration conf)
        throws IOException {

            //ColumnProjectionUtils.setReadAllColumns(conf);
            FileSystem fs =src.getFileSystem(conf) ;
            RCFile.Reader reader = new RCFile.Reader(fs, src, conf);
            //RCFile.Writer writer = new RCFile.Writer(fs, conf, out);
            int result = readerByRow(reader, recoverFlag);
            //int result = readerByCol(reader, recoverFlag);
            reader.close();
            //writer.close();
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
            try {
                reader.getCurrentRow(cols);
                // 包含一列的数据
                BytesRefWritable brw = null;
                Text txt = new Text();
                if (colNum < 0) {
                    colNum = cols.size();
                    System.out.println("file has " + cols.size() + " colunms");
                }
                if (recoverFlag != false){
                    if(writer == null) {
                        conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, colNum);
                        conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, colNum * 1024 * 1024);
                        conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, record_interval); //决定了输出大小和写入速度。设成3时不开压缩50s， 开压缩10分钟， 设置100开压缩一分20s
                        System.out.println("use RECORD_INTERVAL_CONF_STR as "+record_interval);
                        //写入时， 此配置为必须
                        fs = o_path.getFileSystem(conf);
                        writer = new RCFile.Writer(fs, conf, o_path, null,new  DefaultCodec());
                    }
                    /*BytesRefArrayWritable cols_i = new BytesRefArrayWritable(colNum);
                    for (int i = 0; i < colNum; i++) {
                        brw = cols.get(i);
                        BytesRefWritable brw_i = new BytesRefWritable(brw.getData(),0,brw.getLength());
                        cols_i.set(i, brw_i);
                    }*/
                    //System.out.println("column numbers: "+cols.size());
                    //System.out.println("cols:"+ cols.toString());
                   // System.out.println("cols_i:"+ cols_i.toString());
                    writer.append(cols);
                }
            }catch (EOFException e){
                if (writer != null) {
                    System.out.println("handle meet EOF, already recover " + rowID + " records.");
                    writer.close();
                    return 0;
                }
                else{
                    System.out.println("RCFile is bad, " + rowID + " records can be recoverd.");
                    return  1;
                }
            }
            //System.out.println(sb.toString());
        }
        if (writer != null) {
            writer.close();
        }
        return  0;
    }
    protected  int readerByCol(RCFile.Reader reader,boolean recoverFlag) throws IOException {
        // 一个行组的数据
        BytesRefArrayWritable cols = new BytesRefArrayWritable();
        RCFile.Writer writer= null;
        FileSystem fs;
        while (reader.nextBlock()) {
            try {

                for (int count = 0; count < 124; count++) {
                    cols = reader.getColumn(count, cols);
                    if (recoverFlag != false) {
                        if (writer == null) {
                            colNum=124;
                            conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, colNum);
                            conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, colNum * 1024 * 1024);
                            conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 3);
                            //写入时， 此配置为必须
                            fs = FileSystem.get(conf);
                            writer = new RCFile.Writer(fs, conf, o_path);
                        }
                        writer.append(cols);
                    }
                /*BytesRefWritable brw = null;
                Text txt=new Text();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < cols.size(); i++) {
                    brw = cols.get(i);
                    // 根据start 和 length 获取指定行-列数据
                    txt.set(brw.getData(), brw.getStart(), brw.getLength());
                    sb.append(txt.toString());
                    if (i < cols.size() - 1) {
                        sb.append("\t");
                    }
                }
                System.out.println(sb.toString());
                */
                }
            }
                catch (EOFException e) {
                    if (writer != null) {
                       // System.out.println("handle meet EOF, already recover " + rowID + " records.");
                        return 0;
                    }
                }
        }
        return 0;
    }
    public static  void main(String[] args) {
        RCFileHandle rcFileHandler = new RCFileHandle();
        try {
            int result = rcFileHandler.run(args);

            System.exit(result);
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }

    }
}

