package com.tryout.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.Update;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.DeleteMapping;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.params.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tryout.jest.domain.Note;

public class RunMeLocalES {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunMeLocalES.class);
    private static final String NOTES_TYPE_NAME = "notes";
    private static final String CHILE_NOTES_TYPE_NAME = "child_notes1";
    private static final String DIARY_INDEX_NAME = "test_diary";
    private static final String[] indexNameandType = {"test_diary","notes_bulk"};
    private static JestClient jestClient;
    

    public static void main(String[] args) {
        try {
            // Get Jest client
            HttpClientConfig clientConfig = new HttpClientConfig.Builder(
                    "http://localhost:9200").multiThreaded(true).build();
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(clientConfig);
            jestClient = factory.getObject();

            try {
                // run test index & searching
                //RunMeLocalES.deleteTestIndex(jestClient);
                //RunMeLocalES.createTestIndex(jestClient);
                //RunMeLocalES.createMapping(jestClient);
                //RunMeLocalES.indexSomeData(jestClient);
               // RunMeLocalES.readAllData(jestClient);
                //RunMeLocalES.updateTestDataForWholeDoc(jestClient);
                //RunMeLocalES.updateTestForUpdatedFields(jestClient);
            	//RunMeLocalES.getIndexDocById(jestClient);
            	//RunMeLocalES.childMapping(jestClient);
                //RunMeLocalES.subIndexSomeData(jestClient);
            	//RunMeLocalES.updateTestForUpdatedFieldsMap(jestClient);
            	//RunMeLocalES.bulkIndex(jestClient);
            	//RunMeLocalES.bulkIndexwithmymethod(jestClient);
            	//RunMeLocalES.delete(jestClient);
            	//RunMeLocalES.deleteTestIndexWithtype(jestClient);
            	RunMeLocalES.isExistIndex(jestClient);
            } finally {
                // shutdown client
                jestClient.shutdownClient();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void createMapping(JestClient jestClient) throws IOException {
    	String mapSource = "                     \"actuallyUpdatedOn\":{   "
    			+ "                        \"type\":\"date\", "
    			+ "                        \"store\":true, "
    			+ "                        \"format\":\"dateOptionalTime\" "
    			+ "                     }, "
    			+ "                     \"assignUserIds\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"hdmPipeAnalyzer\" "
    			+ "                     }, "
    			+ "                     \"assignUserNames\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"attachments\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"audienceCount\":{   "
    			+ "                        \"type\":\"long\" "
    			+ "                     }, "
    			+ "                     \"audienceMatchType\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"cpmEntityStatus\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"createdBy\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"createdDate\":{   "
    			+ "                        \"type\":\"date\", "
    			+ "                        \"store\":true, "
    			+ "                        \"format\":\"dateOptionalTime\" "
    			+ "                     }, "
    			+ "                     \"customerName\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"departments\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"description\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"vmWhitespaceAnalyzer\" "
    			+ "                     }, "
    			+ "                     \"eventName\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"standard\", "
    			+ "                        \"fields\":{   "
    			+ "                           \"eventNameWhitespace\":{   "
    			+ "                              \"type\":\"string\", "
    			+ "                              \"store\":true, "
    			+ "                              \"norms\":{   "
    			+ "                                 \"enabled\":false "
    			+ "                              }, "
    			+ "                              \"analyzer\":\"vmWhitespaceAnalyzer\" "
    			+ "                           } "
    			+ "                        } "
    			+ "                     }, "
    			+ "                     \"eventName_untouched\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"index\":\"not_analyzed\", "
    			+ "                        \"store\":true "
    			+ "                     }, "
    			+ "                     \"eventSortBy\":{   "
    			+ "                        \"type\":\"date\", "
    			+ "                        \"store\":true, "
    			+ "                        \"format\":\"dateOptionalTime\" "
    			+ "                     }, "
    			+ "                     \"eventStatus\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"eventType\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"eventUpdatedBy\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"event_publishing_type\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"expectedPurchaseDate\":{   "
    			+ "                        \"type\":\"date\", "
    			+ "                        \"store\":true, "
    			+ "                        \"format\":\"dateOptionalTime\" "
    			+ "                     }, "
    			+ "                     \"explanation\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"facilities\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"hdmPipeAnalyzer\" "
    			+ "                     }, "
    			+ "                     \"facilitiesName\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"facilitiesState\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"favourite_ids\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"hdmPipeAnalyzer\" "
    			+ "                     }, "
    			+ "                     \"hdmEntityId\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"hdmUserId\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"id\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"isResponseRequired\":{   "
    			+ "                        \"type\":\"boolean\" "
    			+ "                     }, "
    			+ "                     \"itemAlreadyUsed\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"location\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"matched_ids\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"hdmPipeAnalyzer\" "
    			+ "                     }, "
    			+ "                     \"matchingNotRequired\":{   "
    			+ "                        \"type\":\"boolean\" "
    			+ "                     }, "
    			+ "                     \"originalOpportunityId\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"publishToAll\":{   "
    			+ "                        \"type\":\"boolean\" "
    			+ "                     }, "
    			+ "                     \"purchaseProfileId\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"reasonForRequest\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"responseDeadlineDate\":{   "
    			+ "                        \"type\":\"date\", "
    			+ "                        \"store\":true, "
    			+ "                        \"format\":\"dateOptionalTime\" "
    			+ "                     }, "
    			+ "                     \"responseMatchedIds\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"hdmPipeAnalyzer\" "
    			+ "                     }, "
    			+ "                     \"status\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"keyword\" "
    			+ "                     }, "
    			+ "                     \"submittedDate\":{   "
    			+ "                        \"type\":\"date\", "
    			+ "                        \"store\":true, "
    			+ "                        \"format\":\"dateOptionalTime\" "
    			+ "                     }, "
    			+ "                     \"tags\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"targetedContractDuration\":{   "
    			+ "                        \"type\":\"long\" "
    			+ "                     }, "
    			+ "                     \"totalBedSize\":{   "
    			+ "                        \"type\":\"long\" "
    			+ "                     }, "
    			+ "                     \"unfavourite_ids\":{   "
    			+ "                        \"type\":\"string\", "
    			+ "                        \"store\":true, "
    			+ "                        \"norms\":{   "
    			+ "                           \"enabled\":false "
    			+ "                        }, "
    			+ "                        \"analyzer\":\"hdmPipeAnalyzer\" "
    			+ "                     }, "
    			+ "                     \"unspsc\":{   "
    			+ "                        \"properties\":{   "
    			+ "                           \"id\":{   "
    			+ "                              \"type\":\"string\" "
    			+ "                           }, "
    			+ "                           \"value\":{   "
    			+ "                              \"type\":\"string\" "
    			+ "                           } "
    			+ "                        } "
    			+ "                     }, "
    			+ "                     \"updatedBy\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"updatedDate\":{   "
    			+ "                        \"type\":\"date\", "
    			+ "                        \"store\":true, "
    			+ "                        \"format\":\"dateOptionalTime\" "
    			+ "                     }, "
    			+ "                     \"updatedUserFirstName\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     }, "
    			+ "                     \"updatedUserLastName\":{   "
    			+ "                        \"type\":\"string\" "
    			+ "                     } ";
    	PutMapping putMapping = new PutMapping.Builder(
    			DIARY_INDEX_NAME,
    			NOTES_TYPE_NAME,
    			mapSource
    	).build();
    	JestResult result = jestClient.execute(putMapping);
    	System.out.println("Document : "+result.getJsonString());
		
	}

    
    private static void childMapping(JestClient jestClient) throws IOException{
    	String mapSource =  "{ "
    			+ "	\"_routing\": { "
    			+ "		\"required\": true "
    			+ "	}, "
    			+ "	\"_parent\": { "
    			+ "		\"type\": \"notes\" "
    			+ "	}, "
    			+ "	\"properties\": { "
    			+ "		\"payload\": { "
    			+ "			\"properties\": { "
    			+ " "
    			+ "			} "
    			+ "		} "
    			+ "	} "
    			+ "} ";
    	PutMapping putMapping = new PutMapping.Builder(
    			DIARY_INDEX_NAME,
    			CHILE_NOTES_TYPE_NAME,
    			mapSource
    	).build();
    	JestResult result = jestClient.execute(putMapping);
    	System.out.println("Document : "+result.getJsonString());
    }
    
	private static void getIndexDocById(JestClient jestClient) throws IOException {
    	Get get = new Get.Builder(DIARY_INDEX_NAME, "12345").type(NOTES_TYPE_NAME).build();

    	JestResult result = jestClient.execute(get);
    	
    	System.out.println("Document : "+result.getJsonString());
	}

	private static void updateTestDataForWholeDoc(JestClient jestClient) throws IOException {
    	
    	String source = "{\"userName\": \"sabari\", \"note\": \"Hi All this is my first exp for Jest client\",\"createdOn\":\"2016-02-02T20:53:49+0530\"}";
    	JestResult result = jestClient.execute(
    	            new Index.Builder(source)
    	                    .index(DIARY_INDEX_NAME)
    	                    .type(NOTES_TYPE_NAME)
    	                    .id("AVKilSQUcuRwmt8Y5w_Q")
    	                    .build()
    	    );
    	
    	System.out.println("Updated doc : "+result.toString());
		
	}
    
	private static void updateTestForUpdatedFields(JestClient jestClient) throws IOException {
    	
	String source = "{\"userName\": \"nathan\"}";

	JestResult result = jestClient.execute(new Update.Builder(source).index(DIARY_INDEX_NAME).type(NOTES_TYPE_NAME).id("AVKilSQUcuRwmt8Y5w_Q").build());
    	
    	System.out.println("Updated doc : "+result.toString());
		
	}

	private static void createTestIndex(final JestClient jestClient)
            throws Exception {

		Map<String, String> analysis = new HashMap<String, String>();
		analysis.put("filter"," \"hdmNgramFilter\":{ "
							+ " \"type\":\"nGram\","
							+ " \"min_gram\":\"3\","
							+ " \"max_gram\":\"10\""
							+ " }");
		analysis.put("analyzer", "		\"hdmSemicolonAnalyzer\":{  "
				+ "                     \"pattern\":\"\\;\","
				+ "                     \"flags\":\"DOTALL\","
				+ "                     \"lowercase\":\"true\","
				+ "                     \"type\":\"pattern\","
				+ "                     \"stopwords\":\"_none_\""
				+ "                  },"
				+ "                  \"hdmkstem\":{  "
				+ "                     \"type\":\"custom\","
				+ "                     \"filter\":[  "
				+ "                        \"lowercase\","
				+ "                        \"stop\","
				+ "                        \"kstem\""
				+ "                     ],"
				+ "                     \"tokenizer\":\"standard\""
				+ "                  },"
				+ "                  \"default_search\":{  "
				+ "                     \"pattern\":\"([^\\w]+)|(_)\","
				+ "                     \"type\":\"pattern\","
				+ "                     \"stopwords\":\"_english_\""
				+ "                  },"
				+ "                  \"vmWhitespaceAnalyzer\":{  "
				+ "                     \"pattern\":\"\\s+\","
				+ "                     \"flags\":\"DOTALL\","
				+ "                     \"type\":\"pattern\","
				+ "                     \"lowercase\":\"true\","
				+ "                     \"stopwords\":\"_none_\""
				+ "                  },"
				+ "                  \"default_index\":{  "
				+ "                     \"type\":\"pattern\","
				+ "                     \"pattern\":\"([^\\w]+)|(_)\","
				+ "                     \"stopwords\":\"_english_\""
				+ "                  },"
				+ "                  \"hdmPipeAnalyzer\":{  "
				+ "                     \"flags\":\"DOTALL\","
				+ "                     \"pattern\":\"\\|\","
				+ "                     \"lowercase\":\"true\","
				+ "                     \"type\":\"pattern\","
				+ "                     \"stopwords\":\"_none_\""
				+ "                  },"
				+ "                  \"hdmNGramAnalyzer\":{  "
				+ "                     \"flags\":\"DOTALL\","
				+ "                     \"filter\":[  "
				+ "                        \"lowercase\","
				+ "                        \"stop\""
				+ "                     ],"
				+ "                     \"type\":\"custom\","
				+ "                     \"tokenizer\":\"hdmNGramTokenizer\""
				+ "                  },"
				+ "                  \"hdmURLEmailAnalyzer\":{  "
				+ "                     \"filter\":[  "
				+ "                        \"lowercase\","
				+ "                        \"stop\""
				+ "                     ],"
				+ "                     \"type\":\"custom\","
				+ "                     \"lowercase\":\"true\","
				+ "                     \"tokenizer\":\"uax_url_email\""
				+ "                  }");
		analysis.put("tokenizer","\"hdmNGramTokenizer\":{  \" "
				+ "                     \"type\":\"nGram\",\" "
				+ "                     \"min_gram\":\"3\",\" "
				+ "                     \"max_gram\":\"10\"\" "
				+ "                  }\" ");
        // create new index (if u have this in elasticsearch.yml and prefer
        // those defaults, then leave this out
    
		/**
		 * settingsBuilder support only on above version 2.x
		 */
		/*   Settings.Builder settings = Settings.settingsBuilder();
        settings.put("number_of_shards", 6);
        settings.put("number_of_replicas", 0);
        settings.put("analysis",analysis);
        jestClient.execute(new CreateIndex.Builder(DIARY_INDEX_NAME).settings(
                settings.build().getAsMap()).build());*/
    }

    private static void readAllData(final JestClient jestClient)
            throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("note", "hi"));

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(DIARY_INDEX_NAME).addType(NOTES_TYPE_NAME).build();
        System.out.println(searchSourceBuilder.toString());
        JestResult result = jestClient.execute(search);
        List<Note> notes = result.getSourceAsObjectList(Note.class);
        for (Note note : notes) {
            System.out.println(note);
        }
    }

    private static void deleteTestIndex(final JestClient jestClient)
            throws Exception {
        DeleteIndex deleteIndex = new DeleteIndex.Builder("test")
                .build();
        JestResult result =  jestClient.execute(deleteIndex);
        if(result != null && result.isSucceeded()){
            System.out.println(result.getJsonString());
    	}else {
            System.err.println(result.getJsonString());
		}

    }
    
    private static void isExistIndex(final JestClient jestClient) throws IOException{
    	IndicesExists indicesExists = new IndicesExists.Builder(DIARY_INDEX_NAME).build();
   	 JestResult result =  jestClient.execute(indicesExists);
        if (result != null && result.isSucceeded()) {
            System.out.println("Completed : {}. message {}. " +result.getJsonString());
        } else {
            System.err.println(result.getJsonString());
        }
    }
    
    private static void deleteTestIndexWithtype(final JestClient jestClient)
            throws Exception {
    	DeleteMapping deleteMap = new DeleteMapping.Builder(DIARY_INDEX_NAME, "child_notes").build();
    	 JestResult result =  jestClient.execute(deleteMap);
    	 if(result != null && result.isSucceeded()){
             System.out.println(result.getJsonString());
     	}else {
             System.err.println(result.getJsonString());
 		}


    }

    private static void indexSomeData(final JestClient jestClient)
            throws Exception {
    	Map<String, Object> source = staticMapWithValue();
        Index index = new Index.Builder(source).index(DIARY_INDEX_NAME)
                .type(NOTES_TYPE_NAME).id("71fa9567-14b5-461d-9ef7-b1cbde1b877a").build();
       JestResult result =  jestClient.execute(index);

        System.out.println(result.getJsonString());
    }

	private static Map<String, Object> staticMapWithValue() {
		Map<String, Object> source = new HashMap<String, Object>();
    	Map<String, Object> data = new HashMap<String, Object>();
        // Blocking index
    	data.put(   "eventSortBy","2016-02-03T11:50:33");
    	data.put(   "location","Washington DC");
    	data.put(   "audienceMatchType","MATCH_ANY_TERM");
    	data.put(   "publishToAll",false);
    	data.put(   "audienceCount",null);
    	data.put(   "description","asf dr yreytry");
    	data.put(   "reasonForRequest","");
    	data.put(   "favourite_ids","");
    	data.put(   "createdDate","2016-02-03T11:50:33");
    	data.put(   "responseMatchedIds","");
    	data.put(   "status","DRAFT");
    	data.put(   "originalOpportunityId","");
    	data.put(   "explanation","");
    	data.put(   "facilitiesName","San Joaquin General Hospital");
    	data.put(   "assignUserIds",null);
    	data.put(   "updatedUserLastName","");
    	data.put(   "event_publishing_type","public");
    	data.put(   "createdBy","indijone1@howard.com");
    	data.put(   "purchaseProfileId","469bac87-7578-458f-a9b1-112df68fd77a");
    	data.put(   "facilitiesState","California");
    	data.put(   "submittedDate",null);
    	data.put(   "cpmEntityStatus","ACT");
    	data.put(   "expectedPurchaseDate","2016-02-25T00:00:00");
    	data.put(   "eventName","testing for index testing");
    	data.put(   "hdmEntityId","9d51586e-c281-470c-9f45-b285765c8f65");
    	data.put(   "totalBedSize",236);
    	data.put(   "departments","Intensive Care Unit (ICU)");
    	data.put(   "eventType","BUYER_SOURCING_EVENT");
    	data.put(   "eventUpdatedBy","");
    	data.put(   "updatedBy","indijone1@howard.com");
    	data.put(   "matchingNotRequired",true);
    	data.put(   "id","71fa9567-14b5-461d-9ef7-b1cbde1b877a");
    	data.put(   "unfavourite_ids","");
    	data.put(   "actuallyUpdatedOn","2016-02-03T11:50:33");
    	data.put(   "eventName_untouched","testing for index testing");
    	data.put(   "facilities","HCA155");
    	data.put(   "itemAlreadyUsed","");
    	data.put(   "customerName","Howard Health Hospital");
    	data.put(   "tags","");
    	data.put(   "assignUserNames",null);
    	data.put(   "responseDeadlineDate","2016-02-17T00:00:00");
    	data.put(   "updatedDate","2016-02-03T11:50:33");
    	data.put(   "updatedUserFirstName","");
    	data.put(   "isResponseRequired",false);
    	data.put(   "unspsc",null);
    	data.put(   "matched_ids","");
    	data.put(   "targetedContractDuration",32);
    	data.put(   "hdmUserId","26f12a4a-c8cb-4c81-b0d3-1af7aaf41404");
    	source.put("payload", data);
		return source;
	}
    
    private static void subIndexSomeData(final JestClient jestClient)
            throws Exception {
    	Map<String, Object> source = new HashMap<String, Object>();
    	Map<String, Object> data = new HashMap<String, Object>();
        // Blocking index
    	data.put(   "groupName","fchj");
    	data.put(   "status","ACTIVE");
    	
    	source.put("payload", data);
        Index index = new Index.Builder(source).index("medzo_discussion")
                .type("comment").id("a005b5d5-52bc-44bf-927f-9638f59dd123").setParameter(Parameters.PARENT, "3cd91dd7-7654-4553-a231-3aeb5653f3a2").setParameter(Parameters.ROUTING, "3cd91dd7-7654-4553-a231-3aeb5653f3a2").build();
        JestResult result =  jestClient.execute(index);

        System.out.println(result.getJsonString());
    }
    
    private static void updateTestForUpdatedFieldsMap(JestClient jestClient) throws IOException {
    	
    	Map<String, Object> source = new HashMap<String, Object>();
    	Map<String, Object> data = new HashMap<String, Object>();
    	Map<String, Object> updatedata = new HashMap<String, Object>();
        // Blocking index
    	updatedata.put(   "facilitiesName","ma  aaaaaaaa");
    	updatedata.put(   "location","madurai");
    	/*Get get = new Get.Builder(DIARY_INDEX_NAME, "71fa9567-14b5-461d-9ef7-b1cbde1b877a").type(NOTES_TYPE_NAME).build();
    	JestResult getresult = jestClient.execute(get);
    	data = (Map<String, Object>) getresult.getJsonMap().get("_source");
    	data.putAll(updatedata);*/
    	Map<String, Object> soMap = new LinkedHashMap<String, Object>();
    	soMap.put("payload", updatedata);
    	
    	JestResult result = jestClient.execute(new Update.Builder(soMap).index(DIARY_INDEX_NAME).type(NOTES_TYPE_NAME).id("71fa9567-14b5-461d-9ef7-b1cbde1b877a").build());
        System.out.println("Updated doc : "+result.getJsonString());
    		
    	}
    
    private static void upsert(JestClient jestClient) throws IOException{
    	String script = "{\n" +
    			" \"script\" : \"ctx._source.tags += tag\",\n" +
    			" \"params\" : {\n" +
    			" \"tag\" : \"blue\"\n" +
    			" }\n" +
    			" \"upsert\" : { "+ 
    			" \"tags\" : [\"red\"]" +
    			" }\n" +
    			"}";
    	JestResult result = jestClient.execute(new Update.Builder(script).index(DIARY_INDEX_NAME).type(NOTES_TYPE_NAME).id("71fa9567-14b5-461d-9ef7-b1cbde1b877a").build());
        System.out.println("Updated doc : "+result.getJsonString());
    }
    
    private static void delete(JestClient jestClient) throws IOException{
    	JestResult result = jestClient.execute(new Delete.Builder("AVKntxP6730vtxL4SSlw").index(DIARY_INDEX_NAME).type(NOTES_TYPE_NAME).build());
        System.out.println("delete doc : "+result.getJsonString());
    }
    
    @SuppressWarnings("unchecked")
	private static void bulkIndex(JestClient jestClient) throws IOException{
    	Map<String, Object> source = staticMapWithValue();
    	
    	
    	List<Index> list = new ArrayList<Index>();
    	
    	for (int i=0; i<1;  i++) {
    		list.add(new Index.Builder(source).id(UUID.randomUUID().toString()).build());
		}
    	
    	list.add(new Index.Builder("").id(UUID.randomUUID().toString()).build());
    	
    	Bulk bulk = new Bulk.Builder()
        .defaultIndex(DIARY_INDEX_NAME)
        .defaultType(NOTES_TYPE_NAME)
        .addAction(list)
        .build();
    	JestResult result =jestClient.execute(bulk);
    	Boolean errorResult = (Boolean) result.getValue("errors");
    	if(result.isSucceeded() && errorResult != null && !errorResult ){
            System.out.println("Updated doc : "+result.getJsonString());
           
    	}else {
            System.err.println(result.getJsonString());
            if(result.getValue("items") != null){
            	List<String> failedIds = new ArrayList<String>();
            	for (Map<String, Object> map : (List<Map<String, Object>>) result.getValue("items")) {
					if(map.containsKey("index")){
						Map<String, Object> index = (Map<String, Object>) map.get("index");
						if(index.containsKey("status") &&  (Double)index.get("status") != 201.0){
							failedIds.add((String)index.get("_id"));
						}
					}
				}
            	System.out.println("FAILED IDS :"+failedIds.toString());
            }
		}

    }
    
    private static void bulkIndexwithmymethod(JestClient jestClient) throws IOException{
    	List listOfMap = new ArrayList();
    	for (int i = 0; i < 1; i++) {
    		Map<String, Object> sourcedata = new HashMap<String, Object>();
    		sourcedata.put("_source", staticMapWithValue());
    		sourcedata.put("id", UUID.randomUUID().toString());
			listOfMap.add(sourcedata);
		}
    	bulkIndex(null, listOfMap, 5);
    }
    public static Map<String, String> bulkIndex(String indexNameType, @SuppressWarnings("rawtypes") List documents,int bulkReqSize) {
		

		boolean rollBackBulkIndex = false;
		Map<String, String> failedDocMap = new HashMap<String, String>();
		try {
			int startIndex = 0;
			List<String> indexedIds = new ArrayList<String>();

			do {
				Bulk bulk = new Bulk.Builder()
					.defaultIndex(indexNameandType[0])
					.defaultType(indexNameandType[1])
					.addAction(prepareDocumentIndexs(documents,
							bulkReqSize, startIndex, indexedIds))
					.build();
				
		    	JestResult result = jestClient.execute(bulk);
		    	System.out.println("Batch index startCount="+ startIndex + " EndCount="+ startIndex+bulkReqSize +" Result = "+result.getJsonString());

		    	Boolean errorResult = (Boolean) result.getValue("errors");
		    	if(!result.isSucceeded() || (errorResult != null && errorResult)){
	            	prepareFailedIndexMap(failedDocMap, result);
				}
		    	startIndex = startIndex + bulkReqSize;
		    	//Break the loop if any fail and 'rollBackBulkIndex' true
		    	if (!failedDocMap.isEmpty() && rollBackBulkIndex) {
		    		break;
		    	}
				
			} while (documents != null && documents.size() > startIndex);
			

			if (!failedDocMap.isEmpty() && rollBackBulkIndex) {
				bulkDeleteByIds(indexNameandType, indexedIds, null, true);
			}else if(!failedDocMap.isEmpty()){
				bulkDeleteByIds(indexNameandType, null, failedDocMap, false);
			}

		} catch (Throwable e) {
			System.err.println(e);
		}
		return failedDocMap;
	}

	/**
	 * 
	 * @param indexNameandType
	 * @param indexedIds 
	 * @param failedDocMap
	 * @param isDeleteAll 
	 * @throws Exception
	 */
	private static void bulkDeleteByIds(String[] indexNameandType, List<String> indexedIds, Map<String, String> failedDocMap, boolean isDeleteAll)
			throws Exception {
		List<Delete> deleteIndexs = new ArrayList<Delete>();
		
		if(isDeleteAll){
			for (String indexId : indexedIds) {
				deleteIndexs.add(new Delete.Builder(indexId).index(indexNameandType[0]).type(indexNameandType[1]).build());
			}
		}else {
			for (Map.Entry<String,String> entry : failedDocMap.entrySet()) {
				deleteIndexs.add(new Delete.Builder(entry.getKey()).index(indexNameandType[0]).type(indexNameandType[1]).build());
			}
		}
		
		Bulk bulk = new Bulk.Builder()
		.defaultIndex(indexNameandType[0]).defaultType(indexNameandType[1])
		.addAction(deleteIndexs).build();
		
		JestResult deleteResult = jestClient.execute(bulk);
		if(deleteResult != null){
			System.out.println("After deleted response : "+deleteResult.getJsonString());
		}
	}

	/**
	 * prepare the index list for bulk
	 * @param documents
	 * @param bulkReqSize
	 * @param startIndex
	 * @param indexedIds
	 * @return List<Index>
	 */
	@SuppressWarnings("unchecked")
	private static List<Index> prepareDocumentIndexs(@SuppressWarnings("rawtypes") List documents, int bulkReqSize,
			int startIndex, List<String> indexedIds) {
		List<Index> indexs = new ArrayList<Index>();
		List<Map<String, Object>> limitData = null;
		if(documents.size() >= (startIndex+bulkReqSize)){
			limitData =  documents.subList(startIndex, startIndex+bulkReqSize);
		}else {
			limitData = documents.subList(startIndex, documents.size());
		}
		
		for (Map<String, Object> source : limitData) {
			if (source.containsKey("_source") && source.containsKey("id")) {
				indexs.add(new Index.Builder(source.get("_source")).id((String) source.get("id")).build());
				indexedIds.add((String) source.get("id"));
			}
		}
		indexs.add(new Index.Builder("").id(UUID.randomUUID().toString()).build());
		return indexs;
	}

	/**
	 * prepare the failed indexIds and appropriate error message.
	 * @param failedDocMap
	 * @param result (<"indexId", "Error Message">)
	 */
	@SuppressWarnings("unchecked")
	private static void prepareFailedIndexMap(Map<String, String> failedDocMap, JestResult result) {
		for (Map<String, Object> map : (List<Map<String, Object>>) result.getValue("items")) {
			if(map.containsKey("index")){
				Map<String, Object> index = (Map<String, Object>) map.get("index");
				if(index.containsKey("status") &&  (Double)index.get("status") != 201.0){
					failedDocMap.put((String)index.get("_id"),(String)index.get("error"));
				}
			}
		}
	}
    
    
    
}

