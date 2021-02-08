package com.gsj;

import com.alibaba.fastjson.JSON;
import com.gsj.common.EsConst;
import com.gsj.po.Movie;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

@Slf4j
@SpringBootTest
class ElasticsearchApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    RestHighLevelClient client;

    @Resource
    RestHighLevelClient client1;


    @Test
    void createIndex() throws IOException {
        //创建索引
        CreateIndexRequest java_api = new CreateIndexRequest(EsConst.INDEX_NAME);

        CreateIndexResponse createIndexResponse = client.indices().create(java_api, RequestOptions.DEFAULT);

        log.info(createIndexResponse.index());
        log.info(createIndexResponse.remoteAddress().toString());
        log.info(createIndexResponse.isAcknowledged()+"");
    }

    @Test
    void indexExist() throws IOException {
        //索引存在
        GetIndexRequest getIndexRequest = new GetIndexRequest(EsConst.INDEX_NAME);
        GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        //是否存在
        log.info(exists+"");
        //获取索引信息
        log.info(getIndexResponse.toString());
    }

    @Test
    void deleteIndex() throws IOException {
        //删除索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(EsConst.INDEX_NAME);

        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);

        log.info(delete.toString());
    }

    //添加 一个文档数据
    @Test
    void addOne() throws IOException {
        Movie movie = new Movie();
        movie.setId(1);
        movie.setName("233");
        movie.setCtime(new Date(System.currentTimeMillis()));

        IndexRequest indexRequest=new IndexRequest(EsConst.INDEX_NAME);
        indexRequest.id("1");

        indexRequest.timeout("5s");
        indexRequest.timeout(TimeValue.timeValueSeconds(5L));

        indexRequest.source(JSON.toJSONString(movie), XContentType.JSON);

        IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);

        log.info(index.status().toString());
        log.info(index.getResult().toString());


        //批量插入
        ArrayList<Movie> list = new ArrayList<>();
        list.add(new Movie(2,"通州","万达2",new Date(System.currentTimeMillis())));
        list.add(new Movie(3,"222","万达2北京",new Date(System.currentTimeMillis())));
        list.add(new Movie(4,"草房","上海万达2",new Date(System.currentTimeMillis())));
        list.add(new Movie(5,"666","222 北京",new Date(System.currentTimeMillis())));

        BulkRequest bulkRequest=new BulkRequest(EsConst.INDEX_NAME);
        list.forEach(one->{
            IndexRequest oneIndex=new IndexRequest(EsConst.INDEX_NAME);
            oneIndex.source(JSON.toJSONString(one),XContentType.JSON);
            bulkRequest.add(oneIndex);
        });
        bulkRequest.timeout(TimeValue.timeValueMillis(1L));
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info(bulk.status().toString());



    }
    @Test
    void getOne() throws IOException {
        GetRequest getRequest=new GetRequest(EsConst.INDEX_NAME,"1");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        log.info("是否存在{}",exists);
        GetResponse documentFields = client.get(getRequest, RequestOptions.DEFAULT);
        log.info(documentFields.getSourceAsString());


    }
    @Test
    void updateOne() throws IOException {
        UpdateRequest updateRequest=new UpdateRequest(EsConst.INDEX_NAME,"1");
        updateRequest.timeout(TimeValue.timeValueSeconds(1L));
        updateRequest.doc(JSON.toJSONString(new Movie(66,"666","233",new Date())),XContentType.JSON);
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        log.info(update.status().toString());
    }
    @Test
    void deleteOne() throws IOException {
        DeleteRequest deleteRequest=new DeleteRequest(EsConst.INDEX_NAME,"1");
        deleteRequest.timeout(TimeValue.timeValueSeconds(1L));
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        log.info(delete.status().toString());
    }


    @Test
    void search() throws IOException {
        //查询请求
        SearchRequest searchResponse=new SearchRequest(EsConst.INDEX_NAME);

        //查询构建
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        //条件构造器
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name","233");//精准匹配
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "6");//模糊匹配
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("万达", "name", "address");//模糊匹配

        searchSourceBuilder.query(termQueryBuilder);
//        searchSourceBuilder.query(matchQuery);
//        searchSourceBuilder.query(multiMatchQueryBuilder);
        //查询分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        searchResponse.source(searchSourceBuilder);
        SearchResponse search = client.search(searchResponse, RequestOptions.DEFAULT);

        Arrays.stream(search.getHits().getHits()).forEach(one->{
            Movie movie = JSON.parseObject(one.getSourceAsString(), Movie.class);
            log.info(movie.getName()+" - -  "+movie.getAddress());
        });
    }

}
