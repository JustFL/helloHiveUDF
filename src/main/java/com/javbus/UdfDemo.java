package com.javbus;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class UdfDemo extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("please give me only one arg");
        }

        ObjectInspector argument = objectInspectors[0];
        if (!argument.getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentException("i need primitive type arg");
        }

        PrimitiveObjectInspector primitiveArgument = (PrimitiveObjectInspector) argument;
        if (!primitiveArgument.getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
            throw new UDFArgumentException("i need string type arg");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object object = deferredObjects[0].get();
        if (object == null) {
            return 0;
        }

        return object.toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
