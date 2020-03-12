package io.transwarp.geo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class ConvertDataUDTF extends GenericUDTF {

  private static final int argsCount = 14;
  private static final int retCount = 26;
  private Object forwardColObj[] = new Object[retCount];
  private ObjectInspector[] ois = new ObjectInspector[argsCount];
  private MergeToStay mergeToStay = new MergeToStay();

  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length != argsCount) {
      throw new IllegalArgumentException("there must be " + argsCount + " parameter for ConvertDataUDTF");
    }
    ois[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ois[1] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    ois[2] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ois[3] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ois[4] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ois[5] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ois[6] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
    ois[7] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
    ois[8] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ois[9] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ois[10] = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();
    outFieldNames.add("misidn");
    outFieldNames.add("imsi");
    outFieldNames.add("start_time");
    outFieldNames.add("end_time");
    outFieldNames.add("hour");
    outFieldNames.add("endhour");
    outFieldNames.add("current_lac");
    outFieldNames.add("current_ci");
    outFieldNames.add("current_lat");
    outFieldNames.add("current_lng");
    outFieldNames.add("grid");
    outFieldNames.add("enodebid");
    outFieldNames.add("ci");
    outFieldNames.add("area");
    outFieldNames.add("dura");
    outFieldNames.add("distance");
    outFieldNames.add("speed");
    outFieldNames.add("sequence");
    outFieldNames.add("current_dura");
    outFieldNames.add("roamtype");
    outFieldNames.add("province");
    outFieldNames.add("hrov_id");
    outFieldNames.add("harea_id");
    outFieldNames.add("district_id");
    outFieldNames.add("month_id");
    outFieldNames.add("day_id");
    outFieldNames.add("prov_id");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  public void process(Object[] obs) throws HiveException {
    String deviceNumber = PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(obs[0]);
    List timeList =  ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector).getList(obs[1]);
    List imei_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(obs[2]);
    List imsi_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(obs[3]);
    List lac_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(obs[4]);
    List ci_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(obs[5]);
    List longitude_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaFloatObjectInspector).getList(obs[6]);
    List latitude_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaFloatObjectInspector).getList(obs[7]);
    List prov_id_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(obs[8]);
    List area_id_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(obs[9]);
    List district_id_list = ObjectInspectorFactory.
      getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(obs[10]);
    MergeToStay.ListResult listResult = this.mergeToStay.mergeResult(deviceNumber, timeList, imei_list, imsi_list, lac_list,
      ci_list, longitude_list, latitude_list, prov_id_list, area_id_list, district_id_list);

    forwardColObj[0] = listResult.misidn;
    for (int i = 0; i < listResult.length; i++) {
      if (listResult.imsi.size() > i) forwardColObj[1] = listResult.imsi.get(i);
      if (listResult.start_time.size() > i) forwardColObj[2] = listResult.start_time.get(i);
      if (listResult.end_time.size() > i) forwardColObj[3] = listResult.end_time.get(i);
      if (listResult.hour.size() > i) forwardColObj[4] = listResult.hour.get(i);
      if (listResult.endhour.size() > i) forwardColObj[5] = listResult.endhour.get(i);

      if (listResult.current_lac.size() > i) forwardColObj[6] = listResult.current_lac.get(i);
      if (listResult.current_ci.size() > i) forwardColObj[7] = listResult.current_ci.get(i);

      if (listResult.current_lat.size() > i) forwardColObj[8] = listResult.current_lat.get(i);
      if (listResult.current_lng.size() > i) forwardColObj[9] = listResult.current_lng.get(i);

      if (listResult.grid.size() > i) forwardColObj[10] = listResult.grid.get(i);
      if (listResult.enodebid.size() > i) forwardColObj[11] = listResult.enodebid.get(i);

      if (listResult.ci.size() > i) forwardColObj[12] = listResult.ci.get(i);
      if (listResult.area.size() > i) forwardColObj[13] = listResult.area.get(i);

      if (listResult.dura.size() > i) forwardColObj[14] = listResult.dura.get(i);
      if (listResult.distance.size() > i) forwardColObj[15] = listResult.distance.get(i);

      if (listResult.speed.size() > i) forwardColObj[16] = listResult.speed.get(i);
      if (listResult.sequence.size() > i) forwardColObj[17] = listResult.sequence.get(i);

      if (listResult.current_dura.size() > i) forwardColObj[18] = listResult.current_dura.get(i);
      if (listResult.roamtype.size() > i) forwardColObj[19] = listResult.roamtype.get(i);

      if (listResult.province.size() > i) forwardColObj[20] = listResult.province.get(i);
      if (listResult.hrov_id.size() > i) forwardColObj[21] = listResult.hrov_id.get(i);

      if (listResult.harea_id.size() > i) forwardColObj[22] = listResult.harea_id.get(i);
      if (listResult.district_id.size() > i) forwardColObj[23] = listResult.district_id.get(i);

      if (listResult.month_id.size() > i) forwardColObj[24] = listResult.month_id.get(i);
      if (listResult.day_id.size() > i) forwardColObj[25] = listResult.day_id.get(i);
      if (listResult.prov_id.size() > i) forwardColObj[26] = listResult.prov_id.get(i);
    }

  }

  public void close() throws HiveException {

  }

}
