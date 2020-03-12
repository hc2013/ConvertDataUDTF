package io.transwarp.geo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.FLOAT;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.LONG;

public class TestUDTFN18 extends GenericUDTF {

  private ListObjectInspector[] lois = new ListObjectInspector[2];
  private Object forwardColObj[] = new Object[1];

  @Override
  public StructObjectInspector initialize(ObjectInspector[] obi) throws UDFArgumentException {
    if (((PrimitiveObjectInspector) ((ListObjectInspector) obi[0])
      .getListElementObjectInspector()).getPrimitiveCategory() != LONG) {
      throw new IllegalArgumentException("Wrong！ 1st parameter must be long list");
    }
    if (((PrimitiveObjectInspector) ((ListObjectInspector) obi[1])
      .getListElementObjectInspector()).getPrimitiveCategory() != FLOAT) {
      throw new IllegalArgumentException("Wrong！ 2nd parameter must be float list");
    }
    lois[0] = (ListObjectInspector) obi[0];
    lois[1] = (ListObjectInspector) obi[1];

    List<String> nameList = new ArrayList<>();
    List<ObjectInspector> oiList = new ArrayList<>();
    nameList.add("result");
    oiList.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(nameList, oiList);
  }

  @Override
  public void process(Object[] objects) throws HiveException {
    List list1 = lois[0].getList(objects[0]);
    List list2 = lois[1].getList(objects[1]);
    forwardColObj[0] = test1(list1, list2);;

    forward(forwardColObj);
  }

  private String test1(List<LazyLong> longList, List<LazyFloat> floatList) {
    StringBuilder sb = new StringBuilder();
    long l = longList.get(0).getWritableObject().get();
    float f = floatList.get(0).getWritableObject().get();
    sb.append(test2(l, f));
    return sb.toString();
  }

  private String test2(long l, float f) {
    return l + ";" + f;
  }

  @Override
  public void close() throws HiveException {

  }

}
