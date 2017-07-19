package domob.inf.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;

import java.io.*;

/**
 * Created by domob on 2017/1/9.
 */
public class TXTFileHandler implements FileHandler{
    private File readFile;
    private File writeFile;
    private BufferedReader reader;
    private BufferedWriter writer;
    private boolean readFinishFlag;
    private boolean writeFinishFlag;
    private int colnum;
    public boolean initReader(FileFormatChange.Params args){
        readFile = new File(args.input);
        readFinishFlag = false;
        colnum=-1;
        try {
            reader = new BufferedReader(new FileReader(readFile));
        }catch (FileNotFoundException e){
            System.err.println("input file doesn't exist");
            return false;
        }
        return  true;
    }
    public String readOneRow(){
        String result;
        try {
            result = reader.readLine();
            if (result == null) {
                readFinishFlag = true;
            }
            /*String splits[] = tempString.split("\t");
            colnum=splits.length;
            BytesRefWritable col = null;
            result = new BytesRefArrayWritable(colnum);
            for(int i=0;i<colnum;i++){
                col = new BytesRefWritable(splits[i].getBytes(), 0, splits[i].length());
                result.set(i, col);
            }*/
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }
        return result;
    }
    public boolean readFinish()
    {
        return readFinishFlag;
    }
    public boolean initWriter(/*String Path,Configuration conf*/FileFormatChange.Params args)
    {
        try {
            writeFile = new File(args.output);
            writer = new BufferedWriter(new FileWriter(writeFile));
        }catch (IOException e){
            System.err.println("writer init failed");
            e.printStackTrace();
            return false;
        }

        return true;
    }
    public boolean writeRecords(String row)
    {
        try {
            writer.write(row);
            writer.write("\n");
            writer.flush();
        }catch (Exception e){
            System.err.println("writer meet exception");
            e.printStackTrace();
            return false;
        }

        return true;
    }
    public boolean writeFinish(){
       try{
           writer.close();
        }catch (Exception e){
           System.err.println("writer meet exception");
           e.printStackTrace();
           return false;
        }
        return true;
    }
}
