package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zxysilent/logs"
	"gopkg.in/yaml.v2"
)

var (
	timeStr = time.Now().Format("2006.01.02")
)

type Config struct {
	Global      GlobalConfig `yaml:"global"`
	DataMetrics []CMetrics   `yaml:"metrics"`

	ConfigFile string
}

type GlobalConfig struct {
	ScrapeInterval string `yaml:"scrape_interval"`
	ScrapeTimeout  string `yaml:"scrape_timeout"`
	SearchFrom     int    `yaml:"search_from"`
	SearchSize     int    `yaml:"search_size"`
	LogLevel       string `yaml:"log_level"`
}

type CMetrics struct {
	Name            string            `yaml:"name"`
	Datasource      string            `yaml:"datasource"`
	Index           string            `yaml:"index"`
	BasicAuth       map[string]string `yaml:"basic_auth"`
	TimeRange       string            `yaml:"timerange"`
	OffsetRange     string            `yaml:"offsetrange"`
	Statistics_type string            `yaml:"statistics_type"`
	Metric          string            `yaml:"metric"`
	Project         string            `yaml:"project"`
	Filter          map[string]string `yaml:"filter"`
	Aggregation     []string          `yaml:"aggregation"`
}

type EsSearch struct {
	MustQuery    []elastic.Query
	MustNotQuery []elastic.Query
	ShouldQuery  []elastic.Query
	Filters      []elastic.Query
	Sorters      []elastic.Sorter
	TimeRange    *elastic.RangeQuery
	From         int //分页
	Size         int
	Timeout      string
}

type RtValue struct {
	Value float64
}

type Collector interface {
	Collect(chan<- prometheus.Metric)
	Describe(chan<- *prometheus.Desc)
}

type esMangerOne struct {
	esMangerOneMetric *prometheus.Desc
	config            CMetrics
}

func (e *esMangerOne) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.esMangerOneMetric
}

func (e *esMangerOne) Collect(ch chan<- prometheus.Metric) {
	var totalCount int64 = 0
	if len(e.config.Aggregation) == 1 {
		data := e.Aggselasticsearch(e.ToFilter())
		//fmt.Println(data)
		for _, bucket := range data.Buckets {
			bucketkey := bucket.Key.(string)
			subBucketCount := bucket.DocCount
			ch <- prometheus.MustNewConstMetric(e.esMangerOneMetric, prometheus.GaugeValue, float64(subBucketCount), bucketkey)
		}
	} else if len(e.config.Aggregation) == 0 {
		var labelValues []string
		data, err := e.QueryElasticsearch(e.ToFilter())
		if err != nil {
			log.Println(err)
		}
		//ch <- prometheus.MustNewConstMetric(e.esMangerOneMetric, prometheus.GaugeValue, float64(data), []string{""}...)
		for _, v := range e.config.Filter {
			labelValues = append(labelValues, v)
		}
		ch <- prometheus.MustNewConstMetric(e.esMangerOneMetric, prometheus.GaugeValue, float64(data), labelValues...)
	} else {
		data := e.Aggselasticsearch(e.ToFilter())
		for _, bucket := range data.Buckets {
			bucketkey := bucket.Key.(string)
			subAgg, found := bucket.Aggregations.Terms(e.config.Aggregation[1])
			if !found {
				fmt.Println("subAgg not found: ", e.config.Aggregation[1])
			}

			if len(subAgg.Buckets) == 0 {
				b, err := bucket.Aggregations[e.config.Aggregation[1]].MarshalJSON()
				if err != nil {
					log.Println(err)
				}
				rtv := RtValue{}
				err = json.Unmarshal(b, &rtv)
				ch <- prometheus.MustNewConstMetric(e.esMangerOneMetric, prometheus.GaugeValue, e.Float64Round(rtv.Value), []string{bucketkey, ""}...)
			} else {
				for _, subBucket := range subAgg.Buckets {
					subBucketCount := subBucket.DocCount
					subBucketkey := subBucket.Key.(string)
					totalCount += subBucketCount
					//fmt.Println("key:", key, "bucketkey:", bucketkey, "subBucketkey:", subBucketkey)

					var tempNewrow map[string]interface{} = map[string]interface{}{}
					tempNewrow[e.config.Aggregation[0]] = bucketkey
					tempNewrow[e.config.Aggregation[1]] = subBucketkey
					tempNewrow["count"] = subBucketCount
					//fmt.Println("domain:", bucketkey, "“status”:", subBucketkey, "“count”:", subBucketCount)
					//metrics_name.WithLabelValues(bucketkey, subBucketkey).Add(float64(subBucketCount))
					ch <- prometheus.MustNewConstMetric(e.esMangerOneMetric, prometheus.GaugeValue, float64(subBucketCount), []string{bucketkey, subBucketkey}...)
				}
			}
		}
	}
}
func NewesMangerOneCollector(conf CMetrics) Collector {
	if len(conf.Aggregation) > 0 {
		return &esMangerOne{
			config: conf,
			esMangerOneMetric: prometheus.NewDesc(
				conf.Project+"_"+conf.Metric,
				"nio "+conf.Project+" "+conf.Metric,
				conf.Aggregation,
				nil,
			),
		}
	} else {
		var labels []string
		for k, _ := range conf.Filter {
			labels = append(labels, k)
		}
		return &esMangerOne{
			config: conf,
			esMangerOneMetric: prometheus.NewDesc(
				conf.Project+"_"+conf.Metric,
				"nio "+conf.Project+" "+conf.Metric,
				labels,
				nil,
			),
		}
	}
}

func main() {

	var configFile = "config.yml"
	conf, _ := LoadConfigFile(configFile)

	defer logs.Flush()
	logs.SetLevel(logs.DEBUG)
	logs.SetCallInfo(true)
	logs.SetConsole(true)

	for _, v := range conf.DataMetrics {
		esMangerOneCollector := NewesMangerOneCollector(v)
		prometheus.MustRegister(esMangerOneCollector)
	}

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "OK", http.StatusOK) })
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Log Exporter</title></head>
			<body>
			<h1>Log Exporter</h1>
			<p><a href="/metrics">Metrics</a></p>
			</body>
			</html>`))
	})
	logs.Info("Listening on address", ":9117")
	log.Fatal(http.ListenAndServe(":9117", nil))

}

func LoadConfigFile(configFile string) (*Config, error) {
	buf, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	c := Config{ConfigFile: configFile}
	err = yaml.Unmarshal(buf, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (e *esMangerOne) ToFilter() EsSearch {
	var search EsSearch

	if e.config.OffsetRange == "" {
		e.config.OffsetRange = "0m"
	}

	for k, v := range e.config.Filter {
		//search.Filters = append(search.Filters, elastic.NewMatchQuery(k, v))
		//search.Filters = append(search.Filters, elastic.NewMultiMatchQuery(v, k).Type("phrase"))
		search.ShouldQuery = append(search.ShouldQuery, elastic.NewMatchQuery(k, v))

	}
	if search.TimeRange == nil {
		//search.Filters = elastic.NewRangeQuery("@timestamp").Gte(time.Now().Unix()).Lte(time.Now().Unix() - 3600)
		search.Filters = append(search.Filters, elastic.NewRangeQuery("@timestamp").Gte(fmt.Sprintf("now-%s-%s", e.config.TimeRange, e.config.OffsetRange)).Lte(fmt.Sprintf("now-%s", e.config.OffsetRange)))
	}

	search.From = (1 - 1) * 100
	search.Size = 100
	search.Timeout = "30s"
	return search
}

func (e *esMangerOne) QueryElasticsearch(filter EsSearch) (int64, error) {
	ctx := context.Background()
	index := e.config.Index + "-" + timeStr
	client, err := elastic.NewClient(
		elastic.SetURL(e.config.Datasource),
		elastic.SetBasicAuth(e.config.BasicAuth["username"], e.config.BasicAuth["password"]),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
		// 设置info日志输出
		//elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		logs.Fatal(err)
		return 0, err
	}
	info, code, err := client.Ping(e.config.Datasource).Do(ctx)
	if err != nil {
		logs.Error("elasticsearch is not available", "error", err)
		return 0, err
	}
	logs.Debug("任务:[", e.config.Name, "] connect es return code ", code, " and es version ", info.Version.Number)

	IndexExists, err := client.IndexExists(index).Do(ctx)
	if err != nil {
		logs.Error("", "error", err)
		return 0, err
	}
	if !IndexExists {
		logs.Error("Index not exists")
		return 0, errors.New("Index not exists")
	}

	searchResult := elastic.NewBoolQuery()
	searchResult.Must(filter.MustQuery...)
	searchResult.MustNot(filter.MustNotQuery...)
	searchResult.Should(filter.ShouldQuery...)
	searchResult.Filter(filter.Filters...)
	abody, _ := searchResult.Source()
	aggbody, err := json.Marshal(abody)
	if err != nil {
		return 0, err
	}
	logs.Debug("searchRequest->", "body:", string(aggbody))

	// 当should不为空时，保证至少匹配should中的一项
	if len(filter.MustQuery) == 0 && len(filter.MustNotQuery) == 0 && len(filter.Filters) > 0 {
		searchResult.MinimumShouldMatch("1")
	}
	service := client.Search().Index(index).Query(searchResult).From(filter.From).Size(filter.Size).Timeout(filter.Timeout)
	resp, err := service.Do(ctx)
	if err != nil {
		logs.Error("", "error", err)
	}
	logs.Debug("任务: [", e.config.Name, "] Found a total of ", resp.TotalHits(), "Post")

	if resp.TotalHits() == 0 {
		return 0, nil
	}
	return resp.TotalHits(), nil
}

func (e *esMangerOne) Aggselasticsearch(filter EsSearch) *elastic.AggregationBucketKeyItems {
	ctx := context.Background()
	index := e.config.Index + "-" + timeStr
	client, err := elastic.NewClient(
		elastic.SetURL(e.config.Datasource),
		elastic.SetBasicAuth(e.config.BasicAuth["username"], e.config.BasicAuth["password"]),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
		// 设置info日志输出
		//elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		logs.Fatal(err)
		return nil
	}
	info, code, err := client.Ping(e.config.Datasource).Do(ctx)
	if err != nil {
		logs.Error("elasticsearch is not available", "error", err)
		return nil
	}
	logs.Debug("任务:[", e.config.Name, "] connect es return code ", code, " and es version ", info.Version.Number)
	//log.Printf("任务[%s] connect es return code %d and es version %s\n", e.config.Name, code, info.Version.Number)

	IndexExists, err := client.IndexExists(index).Do(ctx)
	if err != nil {
		logs.Error("", "error", err)
		return nil
	}
	if !IndexExists {
		logs.Error("Index not exists")
		return nil
	}

	aggresult := elastic.NewBoolQuery()
	aggresult.Must(filter.MustQuery...)
	aggresult.MustNot(filter.MustNotQuery...)
	aggresult.Should(filter.ShouldQuery...)
	aggresult.Filter(filter.Filters...)
	abody, _ := aggresult.Source()
	aggbody, err := json.Marshal(abody)
	if err != nil {
		logs.Error("", "error", err)
		return nil
	}
	logs.Debug("searchRequest ->", "body:", string(aggbody))

	if len(e.config.Aggregation) == 0 {
		service, err := client.Search().Index(index).Query(aggresult).SortBy(filter.Sorters...).From(filter.From).Size(filter.Size).Timeout(filter.Timeout).Do(ctx)
		if err != nil {
			logs.Error("", "error", err)
		}
		logs.Debug("任务: [", e.config.Name, "] Found a total of ", service.TotalHits(), "Post")

	} else if len(e.config.Aggregation) == 1 {
		domain := elastic.NewTermsAggregation().Field(e.config.Aggregation[0] + ".keyword").Size(2000).MinDocCount(1)
		service, err := client.Search().Index(index).Query(aggresult).Aggregation(e.config.Aggregation[0], domain).Size(filter.Size).SortBy(filter.Sorters...).Timeout(filter.Timeout).Do(ctx)
		logs.Debug("任务: [", e.config.Name, "] Found a total of ", service.TotalHits(), "Post")
		if err != nil {
			logs.Error("", "error", err)
			return nil
		}
		data, _ := service.Aggregations.Terms(e.config.Aggregation[0])
		return data

	} else if len(e.config.Aggregation) > 1 {
		domain := elastic.NewTermsAggregation().Field(e.config.Aggregation[0] + ".keyword").Size(1000).MinDocCount(1)
		for i := 1; i < len(e.config.Aggregation); i++ {
			domain.SubAggregation(e.config.Aggregation[i], AggregationType(e.config.Statistics_type, e.config.Aggregation[i]))
		}
		service, err := client.Search().Index(index).Query(aggresult).Aggregation("domain", domain).Size(filter.Size).SortBy(filter.Sorters...).Timeout(filter.Timeout).Do(ctx)
		logs.Debug("任务: [", e.config.Name, "] Found a total of ", service.TotalHits(), "Post")

		if err != nil {
			logs.Error("", "error", err)
			return nil
		}
		data, _ := service.Aggregations.Terms(e.config.Aggregation[0])
		return data
	}
	return nil
}

func (e *esMangerOne) Float64Round(f float64) float64 {
	n := math.Pow(10, 2)
	return float64(int(f*n)) / n
}

func AggregationType(typeName string, aggName string) elastic.Aggregation {
	logs.Debug("typeName: ", typeName, "aggName: ", aggName)
	if typeName == "sum" {
		return elastic.NewSumAggregation().Field(aggName)
	} else if typeName == "avg" {
		return elastic.NewAvgAggregation().Field(aggName)
	} else if typeName == "max" {
		return elastic.NewMaxAggregation().Field(aggName)
	} else if typeName == "min" {
		return elastic.NewMinAggregation().Field(aggName)
	} else {
		return elastic.NewTermsAggregation().Field(aggName + ".keyword").Size(2000).MinDocCount(1)
	}
}
