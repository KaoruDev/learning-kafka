package com.kaoruk;

import org.json.JSONException;
import org.json.JSONObject;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by kaoru on 1/6/17.
 */
public class MyTwitter {
    public static final String TOPIC_NAME = "twitter_stream";

    private static boolean initialized = false;
    private static JSONObject twitterConfigs;

    public static void initialize () {
        if (initialized) {
            return;
        }

        try(BufferedReader buffer = new BufferedReader(new FileReader(".secrets.json"))) {
            StringBuilder sb = new StringBuilder();

            buffer.lines().forEach((line) -> {
                sb.append(line);
                sb.append(System.lineSeparator());
            });

            twitterConfigs = new JSONObject(sb.toString()).getJSONObject("twitter");

            initialized = true;
        } catch (FileNotFoundException e){
            System.out.println("WARN: Unable to read .secrets.json");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Something went wrong with reading .secrets.json");
            e.printStackTrace();
        } catch (JSONException e) {
            System.out.println("JSON error!");
            e.printStackTrace();
        }
    }

    public TwitterStream getStream() {
        return new TwitterStreamFactory(getConfigs()).getInstance();
    }

    private Configuration getConfigs() {
        return new ConfigurationBuilder()
                .setOAuthAccessToken(twitterConfigs.getString("access_token"))
                .setOAuthAccessTokenSecret(twitterConfigs.getString("access_token_secret"))
                .setOAuthConsumerKey(twitterConfigs.getString("consumer_key"))
                .setOAuthConsumerSecret(twitterConfigs.getString("consumer_secret"))
                .build();
    }

//    public void fetchAccessToken() {
//        Map<String, String> oauthHeaders = new HashMap<>();
//
//        Date now = new Date();
//        String oauthNounce = DigestUtils.sha1Hex(new Date().toString());
//
//        oauthHeaders.put("oauth_consumer_key", accessToken);
//        oauthHeaders.put("oauth_nounce", oauthNounce);
//        oauthHeaders.put("oauth_signature_method", "HMAC-SHA1");
//        oauthHeaders.put("oauth_timestamp", String.valueOf(now.getTime()));
//        oauthHeaders.put("oauth_token", twitter.getString("access_token"));
//        oauthHeaders.put("oauth_version", "1.0");
//
//
//        MyHttpClient.Builder configs = new MyHttpClient.Builder()
//                .setUrl("https://api.twitter.com/oauth2/token")
//                .setHeaders("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
//                .setParam("grant_type", "client_credentials");
//
//        MyHttpClient.MyResponse response = MyHttpClient.post(configs);
//
//        if (response.ok()) {
//            System.out.println(response.getBody());
//        } else {
//            System.out.println("Request errored =( status: " + response.getStatusCode());
//            System.out.println(response.getBody());
//        }
//    }
//
//
//
//    public String buildOauthHeader(Map<String, String> headers) {
//        List<String> result = new ArrayList<>();
//
//        for(Map.Entry<String, String> header: headers.entrySet()) {
//            result.add(header.getKey() + "=\"" + header.getValue() + "\"");
//        }
//
//        return String.join(",", result);
//    }
}
