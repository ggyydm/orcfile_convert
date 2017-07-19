package domob.inf.hive;

/**
 * Created by domob on 2017/1/6.
 */
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Text;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ORCFileHandler implements FileHandler{
    private Writer orcFileWriter;
    private Reader orcFileReader;
    //private JobConf conf;
    private FileSystem fs;
    private  int orcfileCheckInterval = 300000;
    private  int orcfileStripeSize= 67108864;
    private  String orcfileCompressFormat="SNAPPY";
    private Configuration conf;
    private HiveConf hiveConf;
    private OrcSerde serde;
    private  StructObjectInspector inspector;
    private SettableStructObjectInspector inspectorO;
    private int colNum;
    private List<FieldSchema> colList;
    private String separator;
    RecordReader reader;
    public boolean initWriter(/*String Path,Configuration conf*/FileFormatChange.Params args){
        try {
            conf= new Configuration(true);
            hiveConf = new HiveConf();
            hiveConf.addResource(new Path(args.hive_conf+"/hive-site.xml"));
            separator = args.separator;
            //System.out.println(hiveConf.toString());
            //System.out.println(hiveConf.getAllProperties().toString());
            HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
            String [] hiveTableInfo=args.Hivetable.split("\\.", -1);
            if(hiveTableInfo.length!=2){
                System.err.println("hive table info is needed and should be db.table");
                return false;
            }
           // System.out.println(hiveConf.getAllProperties().toString());
           // System.out.println(client.getAllDatabases().toString());
            colList=client.getSchema(hiveTableInfo[0], hiveTableInfo[1]);

            Table t=client.getTable(hiveTableInfo[0], hiveTableInfo[1]);
            List partitionInfo = t.getPartitionKeys();
            //System.out.println(partitionInfo);
            //TableDesc tableDesc = Utilities.getTableDesc(t);
            Properties p = new Properties();
            StringBuilder columnsName = new StringBuilder();
            StringBuilder columnsType = new StringBuilder();
            StringBuilder schemaString = new StringBuilder();
            schemaString.append("struct<");
            colNum = colList.size()-partitionInfo.size();
            for(FieldSchema f : colList){
                //System.out.println(f.toString());
                if(columnsName.length()>0){
                    columnsName.append(",");
                }
                columnsName.append(f.getName());
                if(columnsType.length()>0){
                    columnsType.append(",");
                }
                columnsType.append(f.getType());
                if(schemaString.length()>7){
                    schemaString.append(",");
                }
                schemaString.append(f.getName()+":"+f.getType());
            }
            schemaString.append(">");
            p.setProperty("columns", columnsName.toString());
            p.setProperty("columns.types", columnsType.toString());
            serde = new OrcSerde();
            serde.initialize(conf, p);
            inspector = (StructObjectInspector) serde.getObjectInspector();

            TypeInfo resultType = TypeInfoUtils.getTypeInfoFromTypeString(schemaString.toString());
            StructObjectInspector inspector3 = (StructObjectInspector) OrcStruct.createObjectInspector(resultType);
            //System.out.println(inspector3.toString());
            //System.out.println(inspector.toString());

                    inspectorO = (SettableStructObjectInspector) serde.getObjectInspector();
            orcFileWriter = OrcFile.createWriter(new Path(args.output),
                    OrcFile.writerOptions(conf)
                            .inspector(inspector3)
                            .stripeSize(orcfileStripeSize)
                            .bufferSize(262144)
                            .compress(CompressionKind.valueOf(orcfileCompressFormat))
                            .version(OrcFile.Version.V_0_11));
        }catch (Exception e){
            System.err.println("orfile writer init failed");
            e.printStackTrace();
            return  false;
        }

        return true;
    }
    public boolean writeRecords(String row){
        if(orcFileWriter==null ){
            System.err.println("init writer first");
            return false;
        }
        try{
            //Text input = new Text(row);
            if(row==null){
                System.err.println("row input is null");
                return true;
            }
            String [] cols = row.split(separator, -1);
            if(cols.length != colNum){
                if(cols.length <= colList.size() && cols.length>colNum){
                    System.out.println("input file contains parition columns info will be ignored");
                }
                else {
                    System.err.println("输入的列数和hive中的列数不一致， 输入的列数为" + cols.length + "，hive中输出的列数为:" + colNum + "输入row：" + row);
                    return false;
                }
            }
            //System.out.println("row"+row);
            //System.out.println("Text"+input.toString());
            //Object orcStruct = serde.deserialize(input);
            //System.out.println("OrcStruct class"+orcStruct.getClass().toString());
            OrcStruct orcStruct2 = (OrcStruct)inspectorO.create();
            List<? extends StructField> flst = inspectorO.getAllStructFieldRefs();
           // System.out.println("orcstruct"+orcStruct.getNumFields());
           /* for (int i =0;i<orcStruct.getNumFields();i++){
                orcStruct[i] = inspectorO.setStructFieldData()
            }*/
            //OrcStruct input = serde.serialize(input, inspector);
            orcInput orcStruct = new orcInput(colNum);
            for (int i =0;i<colNum;i++){
                if(!orcStruct.setFieldValue(i,cols[i],colList.get(i).getType())){
                    return false;
                }
                //System.out.println(cols[i] + " " + colList.get(i).getType());
                inspectorO.setStructFieldData(orcStruct2,flst.get(i),orcStruct.getFieldValue(i));
            }
            orcFileWriter.addRow(orcStruct2);
        }catch (Exception e){
            System.err.println("writer meet exception");
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public boolean writeFinish(){
        try {
            orcFileWriter.close();
        }catch (Exception e){
            System.err.println("writer close exception");
            e.printStackTrace();
        }
        return true;
    }
    public boolean readFinish(){
        try{
            if(reader.hasNext()){
                return false;
            }
        }catch (Exception e){
            System.err.println("orc file reader finish meet exception");
            return true;
        }
        return true;
    }
    public boolean initReader(FileFormatChange.Params args){
        try {
            conf = new Configuration(true);
            hiveConf = new HiveConf();
            hiveConf.addResource(new Path(args.hive_conf + "/hive-site.xml"));
            fs = FileSystem.get(conf);

            orcFileReader = OrcFile.createReader(fs, new Path(args.input));
             reader = orcFileReader.rows(null);

        }catch (Exception e){
            System.err.println("orc file reader init meet exception");
            e.printStackTrace();
        }
        return true;
    }
    public String readOneRow(){
        String result;
        try {
            Object row = null;

            row = reader.next(row);
            StructObjectInspector soi = (StructObjectInspector) orcFileReader.getObjectInspector();
            List<?> orcRow = soi.getStructFieldsDataAsList(row);

            //System.out.println(orcRow);
            result = StringUtils.join(orcRow.toArray(),separator);
        }catch ( Exception e){
            System.err.println("orc file reader meet exception");
            e.printStackTrace();
            return null;
        }
        return  result;
    }
}
