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

import transwarp.org.elasticsearch.common.geo.GeoHashUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by hang.li@transwarp.io on 20-2-14.
 */
public class MergeToStay {

    private final int level = 7;
    private final long minTime = 600000L;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public MergeToStay() {
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    private int[][] mergeToIndex(List<Long> time_list, List<Float> longitude_list, List<Float> latitude_list) {

        List<Integer> startList = new ArrayList<>(), endList = new ArrayList<>();

        if (time_list.size() < 1) {
            return null;    //Check If Trajectory Is Empty
        }

        //Initialize start pivot
        int prevIndex = 0;
        String prevGeoHash = GeoHashUtils
                .stringEncode(longitude_list.get(prevIndex), latitude_list.get(prevIndex), level);
        long duration = 0L;
        for (int i = 1; i < time_list.size(); i++) {
            String curGeoHash = GeoHashUtils
                    .stringEncode(longitude_list.get(i), latitude_list.get(i), level);

            if (curGeoHash.equals(prevGeoHash)) {
                //Accumulate duration
                duration = time_list.get(prevIndex) - time_list.get(i);
            } else {
                //check if duration is longer than 10 minutes
                if (duration >= minTime) {
                    startList.add(prevIndex);   //wrap info in result
                    endList.add(i - 1);
                } else {
                    //discard the points
                }
                prevIndex = i;
                prevGeoHash = GeoHashUtils
                        .stringEncode(longitude_list.get(prevIndex), latitude_list.get(prevIndex), level);
                duration = 0L;
                //sectionDistance = 0.0F;
            }
            //previousLoc = new Float[]{longitude_list.get(i), latitude_list.get(i)};
        }

        if (duration >= minTime) {
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

    public ListResult mergeResult(String device_number, List<Long> time_list, List<String> imei_list
            , List<String> imsi_list, List<String> lac_list, List<String> ci_list, List<Float> longitude_list
            , List<Float> latitude_list, List<String> prov_id_list, List<String> area_id_list
            , List<String> district_id_list) {
        int[][] indexResult = mergeToIndex(time_list, longitude_list, latitude_list);
        int[] start = indexResult[0];
        int[] end = indexResult[1];

        ListResult result = new ListResult(start.length);
        for (int i = 0; i < start.length; i++) {
            result.misidn = device_number;
            //result.imsi.add();
            result.start_time.add(dateFormat.format(time_list.get(start[i])));
            result.end_time.add(dateFormat.format(time_list.get(end[i])));
//            result.hour.add();
//            result.endhour.add();
//            result.current_lac.add();
//            result.current_ci.add();
//            result.current_lat.add();
//            result.current_lng.add();
//            result.grid.add();
//            result.enodebid.add();
//            result.ci.add();
//            result.area.add();
//            result.dura.add();
//            result.distance.add();
//            result.speed.add();
//            result.sequence.add();
//            result.current_dura.add();
//            result.roamtype.add();
//            result.province.add();
//            result.hrov_id.add();
//            result.harea_id.add();
//            result.district_id.add();
//            result.month_id.add();
//            result.day_id.add();
//            result.prov_id.add();
        }
        return result;
    }

    private String getHour(Calendar calendar, long t) {
        calendar.clear();
        calendar.setTimeInMillis(t);
        return String.valueOf(calendar.get(Calendar.HOUR_OF_DAY));
    }

    private float calculateDistance(Float[] pre, float x, float y) {
        double dx = pre[0] - x;
        double dy = pre[1] - y;
        return (float) Math.sqrt(dx * dx + dy * dy);
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