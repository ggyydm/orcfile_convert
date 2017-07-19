package domob.inf.hive;

import org.apache.hadoop.io.*;
import org.apache.commons.lang.StringEscapeUtils;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
//import net.sf.cglib.beans.BeanGenerator;
//import net.sf.cglib.beans.BeanMap;
/**
 * Created by domob on 2017/1/13.
 */
public class orcInput {
    /**
 * 实体Object
 */
public Object object = null;

/**
 * 属性map

public BeanMap beanMap = null;

    public orcInput() {
        super();
    }

    @SuppressWarnings("unchecked")
    public orcInput(Map propertyMap) {
        this.object = generateBean(propertyMap);
        this.beanMap = BeanMap.create(this.object);
    }
 */
    /**
     * 给bean属性赋值
     * @param property 属性名
     * @param value 值

    public void setValue(String property, Object value) {
        beanMap.put(property, value);
    }
     */
    /**
     * 通过属性名得到属性值
     * @param property 属性名
     * @return 值

    public Object getValue(String property) {
        return beanMap.get(property);
    }
     */
    /**
     * 得到该实体bean对象
     * @return
     */

        public Object[] fields ;

        orcInput (int colCount) {
            fields = new Object[colCount] ;
        }

        boolean setFieldValue(int FieldIndex,String value, String type) {
            if(type.equals("string")) {
                String replace = StringEscapeUtils.unescapeJava(value);
                fields[FieldIndex] = new Text(replace);
            }
            else if(value.equals("")){
                fields[FieldIndex] = null;
            }
            else if(type.equals("int")){
                fields[FieldIndex] = new IntWritable(Integer.parseInt(value));
            }
            else if (type.equals("double")){
                fields[FieldIndex] = new DoubleWritable(Double.parseDouble(value));
            }
            else if (type.equals("bigint")){
                fields[FieldIndex] = new LongWritable(Long.parseLong(value));
            }
            else if (type.equals("boolean")){
                fields[FieldIndex] = new BooleanWritable(Boolean.parseBoolean(value));
            }
            else if (type.equals("float")){
                fields[FieldIndex] = new FloatWritable(Float.parseFloat(value));
            }
            else{
                System.err.println("Can't handle hive column type:"+ type+"it is the "+FieldIndex+" column.");
                return false;
            }
            return true;
        }

        void setNumFields(int newSize) {
            if (newSize != fields.length) {
                Object[] oldColumns = fields;
                fields = new Object[newSize];
                System.arraycopy(oldColumns, 0, fields, 0, oldColumns.length);
            }
        }
        Object getFieldValue(int fieldIndex) {
        return this.fields[fieldIndex];
    }
        public int getNumFields() {
        return this.fields.length;
    }

    public Object getObject() {
        return this.object;
    }


/*
    @SuppressWarnings("unchecked")
    private Object generateBean(Map propertyMap) {
        BeanGenerator generator = new BeanGenerator();
        Set keySet = propertyMap.keySet();
        for (Iterator i = keySet.iterator(); i.hasNext();) {
            String key = (String) i.next();
            generator.addProperty(key, (Class) propertyMap.get(key));
        }
        return generator.create();
    }
*/
}
