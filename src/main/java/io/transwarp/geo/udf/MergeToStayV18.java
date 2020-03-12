/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.transwarp.geo.udf;

import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import transwarp.org.elasticsearch.common.geo.GeoHashUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by hang.li@transwarp.io on 20-2-14.
 */
public class MergeToStayV18 {

    private final int level = 7;
    private final long minTime = 600000L;
    private final long minMovingTime = 180000L;
    private final float speedLimit = 15.0F;
    private final transient SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.sss");
    private final transient SimpleDateFormat monthIdFormat = new SimpleDateFormat("yyyyMM");
    private transient Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"), Locale.ROOT);

    public MergeToStayV18() {
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        monthIdFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    private int[][] mergeToIndex(List<LazyLong> time_list, List<LazyFloat> longitude_list, List<LazyFloat> latitude_list) {

        List<Integer> startList = new ArrayList<>(), endList = new ArrayList<>();

        if (time_list.size() < 1) {
            return null;    //Check If Trajectory Is Empty
        }

        //Initialize start pivot
        int prevIndex = 0;
        String prevGeoHash = GeoHashUtils
          .stringEncode(longitude_list.get(prevIndex).getWritableObject().get(),
            latitude_list.get(prevIndex).getWritableObject().get(), level);
        long duration = 0L;
        float distance = 0F, interval = 0F, speed = 0F;
        for (int i = 1; i < time_list.size(); i++) {
            String curGeoHash = GeoHashUtils
              .stringEncode(longitude_list.get(i).getWritableObject().get(),
                latitude_list.get(i).getWritableObject().get(), level);

            if (curGeoHash.equals(prevGeoHash)) {
                //Accumulate duration
                duration = time_list.get(prevIndex).getWritableObject().get()
                  - time_list.get(i).getWritableObject().get();
            } else {
                //check if duration is longer than x minutes
                long timeThreshold = speed > speedLimit ? minMovingTime : minTime;
                if (duration >= timeThreshold) {
                    startList.add(prevIndex);   //wrap info in result
                    endList.add(i - 1);
                } else {
                    //discard the points
                }
                prevIndex = i;
                prevGeoHash = GeoHashUtils
                  .stringEncode(longitude_list.get(prevIndex).getWritableObject().get(),
                    latitude_list.get(prevIndex).getWritableObject().get(), level);
                duration = 0L;

                //calculate the speed diff geohash
                distance = calculateDistance(longitude_list.get(i - 1).getWritableObject().get()
                  , latitude_list.get(i - 1).getWritableObject().get()
                  , longitude_list.get(i).getWritableObject().get(),
                  latitude_list.get(i).getWritableObject().get());
                interval = time_list.get(i).getWritableObject().get() - time_list.get(i - 1).getWritableObject().get();
                speed = calculateSpeed(distance, interval); //calculate speed in m/s
            }
        }

        long timeThreshold = speed > speedLimit ? minMovingTime : minTime;
        if (duration >= timeThreshold) {
            startList.add(prevIndex);   //wrap info in result
            endList.add(time_list.size() - 1);
        }

        int[][] result = new int[2][startList.size()];
        result[0] = getSingle(startList);
        result[1] = getSingle(endList);
        return result;
    }

    private int[] getSingle(List<Integer> list) {
        int[] result = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    public ListResult mergeResult(String device_number, List<LazyLong> time_list, List<LazyString> imei_list
            , List<LazyString> imsi_list, List<LazyString> lac_list, List<LazyString> ci_list, List<LazyFloat> longitude_list
            , List<LazyFloat> latitude_list, List<LazyString> prov_id_list, List<LazyString> area_id_list
            , List<LazyString> district_id_list) {
        int[][] indexResult = mergeToIndex(time_list, longitude_list, latitude_list);
        int[] start = indexResult[0];
        int[] end = indexResult[1];

        ListResult result = new ListResult(start.length);
        for (int i = 0; i < start.length; i++) {
            result.misidn = device_number;
            result.imsi.add(imsi_list.get(start[i]).getWritableObject().toString());
            result.start_time.add(dateFormat.format(time_list.get(start[i])));
            result.end_time.add(dateFormat.format(time_list.get(end[i])));
            result.hour.add(getHour(time_list.get(start[i]).getWritableObject().get()));
            result.endhour.add(getHour(time_list.get(end[i]).getWritableObject().get()));
            result.current_lac.add(lac_list.get(start[i]).getWritableObject().toString());
            result.current_ci.add(ci_list.get(start[i]).getWritableObject().toString());
            result.current_lat.add(latitude_list.get(start[i]).getWritableObject().get());
            result.current_lng.add(longitude_list.get(start[i]).getWritableObject().get());
            String tmpGeoHash = GeoHashUtils
              .stringEncode(longitude_list.get(start[i]).getWritableObject().get(),
                latitude_list.get(start[i]).getWritableObject().get(), level);
            result.grid.add(tmpGeoHash);
            //result.enodebid.add();
            result.ci.add(ci_list.get(start[i]).getWritableObject().toString());
            result.area.add(area_id_list.get(start[i]).toString());
            //result.dura.add();       //该点与下一点时间差，实际测量后感觉不准确
            //result.distance.add();       //该点与下一点的距离， 实际测量后感觉不准确
            //result.speed.add();      //distance/dura 目前缺少这两个
            result.sequence.add(String.valueOf(i + 1));
            //calculate current duration
            long current_dura = time_list.get(end[i]).getWritableObject().get()
              - time_list.get(start[i]).getWritableObject().get();
            result.current_dura.add(current_dura / 1000L);
            //result.roamtype.add();       //lack this field
            result.province.add(prov_id_list.get(start[i]).getWritableObject().toString());
            //result.hrov_id.add();      //归属省份编码, lack this field
            //result.harea_id.add();     //归属地市编码, lack this field
            result.district_id.add(district_id_list.get(start[i]).getWritableObject().toString());
            result.month_id.add(monthIdFormat.format(time_list.get(start[i])));
            result.day_id.add(getDay(time_list.get(start[i]).getWritableObject().get()));
            result.prov_id.add(prov_id_list.get(start[i]).getWritableObject().toString());
        }
        return result;
    }

    private String getHour(long t) {
        calendar.clear();
        calendar.setTimeInMillis(t);
        return String.valueOf(calendar.get(Calendar.HOUR_OF_DAY));
    }

    private String getDay(long t) {
        calendar.clear();
        calendar.setTimeInMillis(t);
        return String.valueOf(calendar.get(Calendar.DAY_OF_MONTH));
    }

    private float calculateDistance(float preX, float preY, float x, float y) {
        double dx = preX - x;
        double dy = preY - y;
        return (float) Math.sqrt(dx * dx + dy * dy);
    }

    private float calculateSpeed(float distance, float interval) {
        float distanceM = distance * 78710; //Meters for the unit
        float intervalSecond = interval / 1000; //Seconds for the unit
        return distanceM / intervalSecond;
    }

    public static class ListResult {

        int length = 0;

        public ListResult(int length) {
            this.length = length;
        }

        String misidn;
        List<String> imsi = new ArrayList<>();
        List<String> start_time = new ArrayList<>();
        List<String> end_time = new ArrayList<>();
        List<String> hour = new ArrayList<>();
        List<String> endhour = new ArrayList<>();
        List<String> current_lac = new ArrayList<>();
        List<String> current_ci = new ArrayList<>();
        List<Float> current_lat = new ArrayList<>();
        List<Float> current_lng = new ArrayList<>();
        List<String> grid = new ArrayList<>();
        List<String> enodebid = new ArrayList<>();
        List<String> ci = new ArrayList<>();
        List<String> area = new ArrayList<>();
        List<Long> dura = new ArrayList<>();
        List<Float> distance = new ArrayList<>();
        List<Float> speed = new ArrayList<>();
        List<String> sequence = new ArrayList<>();
        List<Long> current_dura = new ArrayList<>();
        List<String> roamtype = new ArrayList<>();
        List<String> province = new ArrayList<>();
        List<String> hrov_id = new ArrayList<>();
        List<String> harea_id = new ArrayList<>();
        List<String> district_id = new ArrayList<>();
        List<String> month_id = new ArrayList<>();
        List<String> day_id = new ArrayList<>();
        List<String> prov_id = new ArrayList<>();

    }

}