package domob.inf.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.io.RCFile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

/**
 * Created by domob on 2017/2/6.
 */
public class RCFileHandler implements FileHandler{
    private  RCFile.Reader reader;
    private RCFile.Writer writer;
    private Configuration conf;
    private hiveClient hiveMeta;
    private int record_interval;
    private Path inputPath;
    private LongWritable rowID; //行数
    private boolean readFinishFlag;
    private  boolean hadoopInitFlag = false;
    private String separator;
    int colNum;
    private BytesRefArrayWritable readCols;  //必须定义在这里

    private boolean initHadoopConf(String hivePath){
        conf = new Configuration(true);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        hiveMeta = new hiveClient();
        if(!hiveMeta.init(hivePath)){
            return false;
        }
        hadoopInitFlag = true;
        return true;
    }
    public boolean initReader(FileFormatChange.Params args){
        if(!hadoopInitFlag) {
            initHadoopConf(args.hive_conf);
        }

        try {
            inputPath = new Path(args.input);
            FileSystem fs = inputPath.getFileSystem(conf);
            reader = new RCFile.Reader(fs, inputPath, conf);
            rowID = new LongWritable();
            readCols  = new BytesRefArrayWritable();
            separator =  args.separator;
            readFinishFlag = false;
        }catch (Exception e){
            System.err.println("init rcfile Reader failed");
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public String readOneRow(){
        StringBuilder builder = new StringBuilder();
        try {
            Object row = null;
            // 一个行组的数据
            //BytesRefArrayWritable readCols;  //定义成局部变量不行， 获取行组其他数据会失败， 第一次获取到相应数据， 第二次getCurrentRow, 只调整brw的start位置。 状态保存在reader中， 会和reader信息不一致，导致数据丢失。

            if(reader.next(rowID)){
                reader.getCurrentRow(readCols);
                //System.out.println("readersize:" + readCols.size());
                 //System.out.println("rowId:" + rowID.toString());
                int size = readCols.size();
                BytesRefWritable brw=null;
                for (int i=0;i<size;i++){
                    brw = readCols.get(i);
                    //System.out.println(i);
                    //System.out.println("brw"+brw.toString());
                    Text temp = new Text();
                    temp.set(brw.getData(),brw.getStart(),brw.getLength()); //brw包括行组所有值， 必须制定start和length， 在getCurrentRow会调整
                    //System.out.println("text:"+temp.toString());
                    builder.append(temp.toString());


                    if(i!=size-1){
                        builder.append(separator);
                    }
                    //System.out.println(builder.toString());
                }
            }
            else{
                readFinishFlag = true;
                return null;
            }


        }catch ( Exception e){
            System.err.println("rc file reader meet exception");
            e.printStackTrace();
            return null;
        }

        return  builder.toString();
    }
    public boolean readFinish()
    {
        return readFinishFlag;
    }
    public boolean initWriter(/*String Path,Configuration conf*/FileFormatChange.Params args)
    {
        if(!hadoopInitFlag && !initHadoopConf(args.hive_conf)){
            return  false;
        }
        if(args.record_interval==null){
            record_interval=100;
        }
        else{
            record_interval=Integer.parseInt(args.record_interval);
        }

        colNum = hiveMeta.tableColumnNum(args.Hivetable);
       // System.out.println("writersize:"+colNum);
        if( colNum == -1 ){
            System.err.println("hive metastore get infomation meet error");
            return  false;
        }
        conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, colNum);
        conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, colNum * 1024 * 1024);
        conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, record_interval);
        try {
            Path o_path = new Path(args.output);
            FileSystem fs = o_path.getFileSystem(conf);
            writer = new RCFile.Writer(fs, conf, o_path, null, new DefaultCodec());
        }catch (Exception e){
            System.err.println("rcfile writer init failed");
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public boolean writeFinish(){
        try {
            writer.close();
        }catch (Exception e){
            System.err.println("writer close exception");
            e.printStackTrace();
        }
        return true;
    }
    public boolean writeRecords(String row) {
        if (writer == null) {
            System.err.println("init writer first");
            return false;
        }
        String[] cols = row.split(separator, -1);
        if (cols.length != colNum) {
            System.err.println("输入的列数和hive中的列数不一致， 输入的列数为" + cols.length + "，hive中输出的列数为:" + colNum + "输入row：" + row);
            return false;

        }
        //System.out.println("row"+row);
        try {


            BytesRefArrayWritable cols_i = new BytesRefArrayWritable(colNum);
            for (int i = 0; i < colNum; i++) {
                BytesRefWritable brw_i = new BytesRefWritable(cols[i].getBytes(), 0, cols[i].length());
                cols_i.set(i, brw_i);
            }
            writer.append(cols_i);
        } catch (Exception e) {
            System.err.println("writer meet exception");
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
