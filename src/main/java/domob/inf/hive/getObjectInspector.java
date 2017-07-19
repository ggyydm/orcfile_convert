package domob.inf.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Created by domob on 2017/1/13.
 */
public class getObjectInspector {
    public ObjectInspector transObjectInspector(String typeString){
        ObjectInspector result;
        if(typeString.equals("int")){
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        }
        else if(typeString.equals("boolean")){
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
        }
        else if(typeString.equals("string")){
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BYTE);
        }else if(typeString.equals("BOOLEAN")){
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
        }else if(typeString.equals("bigint")){
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
        }else if(typeString.equals("double")){
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
        }else if(typeString.equals("float")){
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.FLOAT);
        }
        else{
            return null;
        }
        return result;
    }
}
