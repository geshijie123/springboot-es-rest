package com.gsj.controller;

import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gsj.common.EsConst;
import com.gsj.po.Cinema;
import com.gsj.po.Movie;
import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@RestController
public class DemoController {

    private static String CINEMA_INDEX= "java_cinema";


    @Autowired
    RestHighLevelClient restHighLevelClient;

    @RequestMapping("add")
    public Object add() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(CINEMA_INDEX);
        // 创建mapping
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                            .startObject("local")
                                .field("type","geo_point")
                            .endObject()
                    .endObject()
                .endObject();
        System.out.println(mapping.toString());
        createIndexRequest.mapping("location",mapping);
        restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        FileReader fileReader = new FileReader("B:/home/cinema.json");
        JSONObject jsonObject = JSON.parseObject(fileReader.readString());
        JSONArray records = jsonObject.getJSONArray("RECORDS");


        BulkRequest bulkRequest=new BulkRequest(CINEMA_INDEX);
        records.forEach(one->{
            IndexRequest oneIndex=new IndexRequest(CINEMA_INDEX);
            JSONObject parse = JSONObject.parseObject(one.toString());
            parse.put("local",parse.getBigDecimal("lat")+","+parse.getBigDecimal("lon"));
            oneIndex.source(parse.toJSONString(), XContentType.JSON);
            bulkRequest.add(oneIndex);
        });
        bulkRequest.timeout(TimeValue.timeValueMillis(1L));
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        return bulk.status();
    }


    @RequestMapping("search")
    public Object search(String world) throws IOException {

        //查询请求
        SearchRequest searchResponse=new SearchRequest(CINEMA_INDEX);



        //查询构建
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        //条件构造器
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(world, "area_name","name", "address");//模糊匹配


        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name");
        highlightBuilder.field("area_name");
        highlightBuilder.field("address");
        highlightBuilder.requireFieldMatch(true);
        highlightBuilder.preTags("<font style='color:red'>");
        highlightBuilder.postTags("</font>");

        searchSourceBuilder.highlighter(highlightBuilder);
        searchSourceBuilder.query(multiMatchQueryBuilder);
        //查询分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(20);

        searchResponse.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchResponse, RequestOptions.DEFAULT);
        List<Map<String, Object>> collect = Arrays.stream(search.getHits().getHits()).parallel().map(one -> {
            Map<String, Object> sourceAsMap = one.getSourceAsMap();

            Map<String, HighlightField> highlightFields = one.getHighlightFields();

            if (highlightFields.get("name") != null) {
                Text[] names = highlightFields.get("name").fragments();
                String new_name = "";
                for (Text name : names) {
                    new_name += name;
                }
                sourceAsMap.put("name", new_name);
            }
            if (highlightFields.get("area_name") != null) {
                Text[] names = highlightFields.get("area_name").fragments();
                String new_area_name = "";
                for (Text name : names) {
                    new_area_name += name;
                }
                sourceAsMap.put("area_name", new_area_name);
            }
            if (highlightFields.get("address") != null) {
                Text[] names = highlightFields.get("address").fragments();
                String new_address = "";
                for (Text name : names) {
                    new_address += name;
                }
                sourceAsMap.put("address", new_address);
            }


            return sourceAsMap;
        }).collect(Collectors.toList());
        return collect;
    }
}
