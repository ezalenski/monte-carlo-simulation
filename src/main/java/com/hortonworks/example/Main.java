package com.hortonworks.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;

import scala.Int;
import scala.Tuple2;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;


/**
 * TODO
 * add functions
 * add a start and stop timer for benchmarking
 * add kryo serializer
 */
public class Main {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
            .builder()
            .appName("monte-carlo-var-calculator")
            .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        /*
          Initializations
        */
        final int NUM_TRIALS = 5;
        String listOfCompanies = new File("companies_list.txt").toURI().toString();
        String start_date = "1990-01-01";
        String end_date = "2017-11-30";
        String url = "https://www.quandl.com/api/v3/datasets/WIKI/";
        String tickerUrl = "https://www.quandl.com/api/v3/datatables/WIKI/PRICES.csv";
        String API_KEY;

        if (args.length > 0) {
            listOfCompanies = args[0];
        }
        if (args.length > 1) {
            API_KEY = args[1];
        } else {
            API_KEY = "fazBfG5rze4V5zxB-qkQ";
        }

        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(tickerUrl + String.format("?date=%s&qopts.columns=ticker,close&api_key=%s", end_date, API_KEY));
        HttpResponse response = client.execute(request);
        BasicResponseHandler handler = new BasicResponseHandler();
        List<String> bestTickers = Arrays.asList(handler.handleResponse(response).trim().toString().split("\n"))
            .stream()
            .filter(line -> !line.contains("ticker"))
            .map(line -> {
                    String[] splits = line.split(",", -2);
                    String symbol = splits[0];
                    Double price = new Double(splits[1]);
                    return new Tuple2<>(symbol, price);
                })
            .sorted((a, b) -> Double.compare(b._2(), a._2()))
            .limit(25)
            .map(t -> t._1())
            .collect(Collectors.toList());

        //bestTickers.forEach(t -> System.out.println(t));
        /*
          read a list of stock symbols and their weights in the portfolio, then transform into a Map<Symbol,Weight>
          1. read in the data, ignoring header
          2. convert dollar amounts to fractions
          3. create a local map
        */
        JavaRDD<String> filteredFileRDD = jsc.textFile(listOfCompanies).filter(s -> !s.startsWith("#") && !s.trim().isEmpty());
        filteredFileRDD = filteredFileRDD.union(jsc.parallelize(bestTickers));
        JavaPairRDD<String, Double> symbolsAndWeightsRDD = filteredFileRDD.filter(s -> !s.startsWith("Symbol")).mapToPair(s -> {
                String[] splits = s.split(",", -2);
                return new Tuple2<>(splits[0], 1.0);
            });

        //convert from $ to % weight in portfolio
        Double totalInvestment = 1000.0;
        Map<String, Double> symbolStartPrices = symbolsAndWeightsRDD.mapToPair(t -> {
                DefaultHttpClient c = new DefaultHttpClient();
                HttpGet req = new HttpGet(url + String.format("%s.csv?column_index=4&start_date=%s&end_date=%s&api_key=%s", t._1(), start_date, end_date, API_KEY));
                HttpResponse res = c.execute(req);
                BasicResponseHandler h = new BasicResponseHandler();
                try {
                    return Arrays.asList(h.handleResponse(res).trim().toString().split("\n"))
                        .stream()
                        .filter(line -> {
                                if(!line.contains("Date")) {
                                    String[] splits = line.split(",", -2);
                                    return !splits[splits.length-1].isEmpty();
                                }
                                return false;
                            })
                        .limit(1)
                        .map(line -> {
                                String[] splits = line.split(",", -2);
                                String symbol = t._1();
                                Double price = new Double(splits[splits.length-1]);
                                return new Tuple2<>(t._1(), price);
                            })
                        .collect(Collectors.toList()).get(0);
                } catch(HttpResponseException e) {
                    System.out.println(e.getStatusCode());
                    throw new Exception("WTF");
                }
            }).collectAsMap();

        //debug
        System.out.println("symbolStartPrices");
        symbolStartPrices.forEach((s, d) -> System.out.println("symbol: " + s + ", price: " + d));

        /*
          read all stock trading data, and transform
          1. get a PairRDD of date -> (symbol, changeInPrice)
          2. reduce by key to get all dates together
          3. filter every date that doesn't have the max number of symbols
          \        */

        // 1. get a PairRDD of date -> Tuple2(symbol, changeInPrice)

        List<Tuple2<String, Tuple2<String, Double>>> datesToSymbolsAndChangeList = symbolsAndWeightsRDD.map(t -> {
                DefaultHttpClient c = new DefaultHttpClient();
                HttpGet req = new HttpGet(url + String.format("%s.csv?start_date=%s&end_date=%s&transform=rdiff&api_key=%s", t._1(), start_date, end_date, API_KEY));
                HttpResponse res = c.execute(req);
                BasicResponseHandler h = new BasicResponseHandler();
                try {
                    return Arrays.asList(h.handleResponse(res).trim().toString().split("\n"))
                        .stream()
                        .filter(line -> {
                                if(!line.contains("Date")) {
                                    String[] splits = line.split(",", -2);
                                    return !splits[splits.length-1].isEmpty();
                                }
                                return false;
                            })
                        .map(line -> {
                        String[] splits = line.split(",", -2);
                        String symbol = t._1();
                        String date = splits[0];
                        Double changeInPrice = new Double(splits[splits.length-1]);
                        return new Tuple2<>(date, new Tuple2<>(symbol, changeInPrice));
                            })
                        .collect(Collectors.toList());
                } catch(HttpResponseException e) {
                    System.out.println(e.getStatusCode());
                    throw new Exception("WTF");
                }
            }).collect()
            .stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());

        //debug
        //System.out.println(datesToSymbolsAndChangeList.get(0));

        //2. reduce by key to get all dates together
        JavaPairRDD<String, Tuple2<String, Double>> datesToSymbolsAndChangeRDD = jsc.parallelize(datesToSymbolsAndChangeList).mapToPair(x -> x);
        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> groupedDatesToSymbolsAndChangeRDD = datesToSymbolsAndChangeRDD.groupByKey();
        //debug
        //System.out.println(groupedDatesToSymbolsAndChangeRDD.first()._1() + "->" + groupedDatesToSymbolsAndChangeRDD.first()._2());

        //3. filter every date that doesn't have the max number of symbols
        long numSymbols = symbolsAndWeightsRDD.count();
        Map<String, Long> countsByDate = datesToSymbolsAndChangeRDD.countByKey();
        //System.out.println("num symbols: " + numSymbols);
        //System.out.println(countsByDate);
        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> filterdDatesToSymbolsAndChangeRDD = groupedDatesToSymbolsAndChangeRDD.filter(x -> (Long) countsByDate.get(x._1()) >= numSymbols);
        long numEvents = filterdDatesToSymbolsAndChangeRDD.count();
        //debug
        //filterdDatesToSymbolsAndChangeRDD.take(10).forEach(x -> System.out.println(x._1() + "->" + x._2()));

        if (numEvents < 1) {
            System.out.println("Not enough trade data");
            spark.stop();
            System.exit(0);
        }

        /*
          execute NUM_TRIALS
          1. pick a random date from the list of historical trade dates
          2. sum(stock weight in overall portfolio * change in price on that date)
        */
        List<Integer> l = new ArrayList<>(NUM_TRIALS);
        for (int i = 0; i < NUM_TRIALS; i++) {
            l.add(i);
        }

        List<JavaPairRDD<String, Double>> trialResults = l.stream().map(i -> {
                int numDays = 365;
                double fraction = 1.0 * numDays / numEvents;
                return filterdDatesToSymbolsAndChangeRDD.sample(true, fraction)
                .map(t -> t._2())
                .flatMapToPair(x -> x.iterator())
                .reduceByKey((agg, dayChange) -> agg + dayChange);
            }).collect(Collectors.toList());

        //debug
        //System.out.println("events: " + numEvents);
        //System.out.println("fraction: " + fraction);
        //System.out.println("total runs: " + resultOfTrials.count());

        /*
          create a temporary table out of the data and take the 5%, 50%, and 95% percentiles

          1. multiple each float by 100
          2. create an RDD with Row types
          3. Create a schema
          4. Use that schema to create a data frame
          5. execute Hive percentile() SQL function
        */

        JavaRDD<Row> resultOfTrialsRows = trialResults
            .stream()
            .map(rdd -> rdd.map(x -> RowFactory.create(x._1(), Math.round(x._2() / 100 * symbolStartPrices.get(x._1()) ))))
            .reduce(jsc.emptyRDD(), (a, b) -> a.union(b));
        StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("symbol", DataTypes.StringType, false), DataTypes.createStructField("changePct", DataTypes.LongType, false)});
        Dataset<Row> resultOfTrialsDF = spark.createDataFrame(resultOfTrialsRows, schema);
        resultOfTrialsDF.registerTempTable("results");
        Map<String, Tuple2<Double, Double>> stockValues = spark
            .sql("select symbol, avg(changePct) from results group by symbol")
            .toJavaRDD()
            .mapToPair(r -> new Tuple2<>((String)r.get(0), new Tuple2<>(symbolStartPrices.get(r.get(0)), (Double)r.get(1))))
            .collectAsMap();
        List<Tuple2<String,Integer>> result = knapsack(totalInvestment, stockValues);
        Map<String,Long> reducedResults = jsc.parallelize(result)
            .mapToPair(x -> x)
            .countByKey();

        Double gain = 0.0;
        List<String> finalStocks = new ArrayList<String>(reducedResults.keySet());
        for(String stock : finalStocks) {
            gain += stockValues.get(stock)._2() * reducedResults.get(stock);
        }
        System.out.println("Investing in the following stocks:");
        reducedResults.forEach((s, c) -> System.out.println(s + ": " + c));
        System.out.println("On average will gain $" + gain + " or " + gain/totalInvestment + "%");

        spark.stop();
    }

    /**
     * Knapsack Implementation
     * @param money
     * @param map
     * @return
     */
    private static List<Tuple2<String,Integer>> knapsack(Double money, Map<String, Tuple2<Double, Double>> map){
        List<Tuple2<Integer,List<Tuple2<String, Integer>>>> dp = new ArrayList<>();

        List<String> keyList = new ArrayList<String>(map.keySet());
        int len = keyList.size();

        for(int i = 0; i <= money; i++){
            String keyI = keyList.get(i);
            Tuple2<Integer,List<Tuple2<String, Integer>>> empty = new Tuple2<>(0, new ArrayList<>());
            dp.add(empty);

            for(int j = 0; j < len; j++){
                String keyJ = keyList.get(j);
                if(Math.round(map.get(keyJ)._1) <= i){
                    Tuple2<Integer,List<Tuple2<String, Integer>>> firstItem = deepCopy(dp.get(i));
                    Tuple2<Integer,List<Tuple2<String, Integer>>> secondItem = deepCopy(dp.get(i - (int)Math.round(map.get(keyJ)._1)));
                    secondItem = appendToList(secondItem, keyJ, (int)Math.round(map.get(keyJ)._2));
                    if (!max(firstItem, secondItem)) {
                        dp.remove(i);
                        dp.add(secondItem);
                    }
                }
            }
        }
        return dp.get(dp.size()-1)._2();
    }

    private static boolean max(Tuple2<Integer,List<Tuple2<String, Integer>>> t1,
                                                      Tuple2<Integer,List<Tuple2<String, Integer>>> t2){
        if(t1._1 >= t2._1){
            return true;
        }
        else {
            return false;
        }
    }



    private static Tuple2<Integer,List<Tuple2<String, Integer>>> deepCopy(Tuple2<Integer,List<Tuple2<String, Integer>>> obj) {
        Tuple2<Integer,List<Tuple2<String, Integer>>> item = new Tuple2<>(obj._1,
                new ArrayList<Tuple2<String, Integer>>());

        for (Tuple2<String, Integer> tuple : obj._2) {
            item._2.add(new Tuple2<>(tuple._1, tuple._2));
        }

        return item;
    }

    private static Tuple2<Integer,List<Tuple2<String, Integer>>> appendToList(Tuple2<Integer,List<Tuple2<String, Integer>>> item, String symbol, double val) {
        List<Tuple2<String, Integer>> array = item._2;

        Tuple2<String, Integer> newTuple = new Tuple2<>(symbol, 1);
        array.add(newTuple);

        Tuple2<Integer,List<Tuple2<String, Integer>>> newItem = new Tuple2<>(item._1 + (int) Math.round(val), array);

        return newItem;
    }
}
