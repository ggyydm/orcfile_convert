package domob.inf.hive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * Created by domob on 2017/2/7.
 */
public class hiveClient {
    private HiveConf hiveConf;
    private HiveMetaStoreClient client;
    public boolean init(String confFolder){
        hiveConf = new HiveConf();
        hiveConf.addResource(new Path(confFolder+"/hive-site.xml"));
        //System.out.println(hiveConf.getAllProperties().toString());
        try {
            client = new HiveMetaStoreClient(hiveConf);
        }
        catch ( Exception e){
            System.err.println("hive metastore init failed");
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public int tableColumnNum(String tableName){
        String[] hiveTableInfo = tableName.split("\\.");
        if (hiveTableInfo.length != 2) {
            System.err.println("hive table info is needed and should be db.table");
            return -1;
        }
        int result ;
        try {
            List<FieldSchema> colList = client.getSchema(hiveTableInfo[0], hiveTableInfo[1]); //所有列信息， 包括partition列
            Table t = client.getTable(hiveTableInfo[0], hiveTableInfo[1]);
            List partitionInfo = t.getPartitionKeys();
            result = colList.size() - partitionInfo.size();
        }catch (Exception e){
            System.err.println("hvie get table info failed");
            e.printStackTrace();
            return -1;
        }
        return  result;
    }
}
