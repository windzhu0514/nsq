// 对nsq和nsqlookupd的一些查询操作
package clusterinfo

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/blang/semver"
	"github.com/windzhu0514/nsq/internal/http_api"
	"github.com/windzhu0514/nsq/internal/lg"
	"github.com/windzhu0514/nsq/internal/stringy"
)

type PartialErr interface {
	error
	Errors() []error
}

type ErrList []error

func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

func (l ErrList) Errors() []error {
	return l
}

type ClusterInfo struct {
	log    lg.AppLogFunc
	client *http_api.Client // http.Client nsqd作为一个客户端和其他服务通信
}

// 创建一个集群信息
func New(log lg.AppLogFunc, client *http_api.Client) *ClusterInfo {
	return &ClusterInfo{
		log:    log,
		client: client,
	}
}

func (c *ClusterInfo) logf(f string, args ...interface{}) {
	if c.log != nil {
		c.log(lg.INFO, f, args...)
	}
}

// 查询nsqd nsqlookup的版本
// GetVersion returns a semver.Version object by querying /info
func (c *ClusterInfo) GetVersion(addr string) (semver.Version, error) {
	endpoint := fmt.Sprintf("http://%s/info", addr)
	var resp struct {
		Version string `json:"version"`
	}
	err := c.client.GETV1(endpoint, &resp)
	if err != nil {
		return semver.Version{}, err
	}
	if resp.Version == "" {
		resp.Version = "unknown"
	}
	return semver.Parse(resp.Version)
}

// GetLookupdTopics 返回从所有给定的nsqlookupd地址查询到的所有topic的集合
// GetLookupdTopics returns a []string containing a union of all the topics
// from all the given nsqlookupd
func (c *ClusterInfo) GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []string `json:"topics"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/topics", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			topics = append(topics, resp.Topics...)
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	topics = stringy.Uniq(topics) // 去重
	sort.Strings(topics)

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetLookupdTopicChannels 从所有给定的lookupd地址查询给定的topic下的有channel
// GetLookupdTopicChannels returns a []string containing a union of all the channels
// from all the given lookupd for the given topic
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	var channels []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Channels []string `json:"channels"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			channels = append(channels, resp.Channels...)
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	channels = stringy.Uniq(channels)
	sort.Strings(channels)

	if len(errs) > 0 {
		return channels, ErrList(errs)
	}
	return channels, nil
}

// GetLookupdProducers 从指定的lookupd地址查询所有nsqd中的生产者
// GetLookupdProducers returns Producers of all the nsqd connected to the given lookupds
func (c *ClusterInfo) GetLookupdProducers(lookupdHTTPAddrs []string) (Producers, error) {
	var producers []*Producer
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	producersByAddr := make(map[string]*Producer)
	maxVersion, _ := semver.Parse("0.0.0") // 记录最大版本

	type respType struct {
		Producers []*Producer `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/nodes", addr)
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, producer := range resp.Producers {
				key := producer.TCPAddress()
				p, ok := producersByAddr[key]
				if !ok {
					producersByAddr[key] = producer
					producers = append(producers, producer)
					if maxVersion.LT(producer.VersionObj) {
						maxVersion = producer.VersionObj
					}
					sort.Sort(producer.Topics)
					p = producer
				}
				p.RemoteAddresses = append(p.RemoteAddresses,
					fmt.Sprintf("%s/%s", addr, producer.Address()))
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	// 检查生产者是否过期
	for _, producer := range producersByAddr {
		if producer.VersionObj.LT(maxVersion) {
			producer.OutOfDate = true
		}
	}
	sort.Sort(ProducersByHost{producers})

	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetLookupdTopicProducers 在从给定的lookupd地址查询到的所有nsqd结点中查询指定topic的生产者
// GetLookupdTopicProducers returns Producers of all the nsqd for a given topic by
// unioning the nodes returned from the given lookupd
func (c *ClusterInfo) GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Producers Producers `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			// 过滤地址相同的生产者
			for _, p := range resp.Producers {
				for _, pp := range producers {
					if p.HTTPAddress() == pp.HTTPAddress() {
						goto skip
					}
				}
				producers = append(producers, p)
			skip:
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDTopics 从给定的nsqd地址查询所有生成出topic
// GetNSQDTopics returns a []string containing all the topics produced by the given nsqd
func (c *ClusterInfo) GetNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topics = stringy.Add(topics, topic.Name)
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Strings(topics)

	if len(errs) > 0 {
		return topics, ErrList(errs)
	}
	return topics, nil
}

// GetNSQDProducers 从所有给定的nsqd地址查询nsqd生产者的信息
// GetNSQDProducers returns Producers of all the given nsqd
func (c *ClusterInfo) GetNSQDProducers(nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/info", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var infoResp infoRespType
			err := c.client.GETV1(endpoint, &infoResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			// 查询该nsqd所有topic状态
			endpoint = fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err = c.client.GETV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			version, err := semver.Parse(infoResp.Version)
			if err != nil {
				version, _ = semver.Parse("0.0.0")
			}

			lock.Lock()
			defer lock.Unlock()
			producers = append(producers, &Producer{
				Version:          infoResp.Version,
				VersionObj:       version,
				BroadcastAddress: infoResp.BroadcastAddress,
				Hostname:         infoResp.Hostname,
				HTTPPort:         infoResp.HTTPPort,
				TCPPort:          infoResp.TCPPort,
				Topics:           producerTopics,
			})
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDTopicProducers returns Producers containing the addresses of all the nsqd
// that produce the given topic
func (c *ClusterInfo) GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) (Producers, error) {
	var producers Producers
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			// 查询指定nsqd的指定topic的状态
			endpoint := fmt.Sprintf("http://%s/stats?format=json&topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqd %s", endpoint)

			var statsResp statsRespType
			err := c.client.GETV1(endpoint, &statsResp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			// 所有topic
			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			// 查找指定的topic所在的nsqd的信息
			for _, t := range statsResp.Topics {
				if t.Name == topic {
					endpoint := fmt.Sprintf("http://%s/info", addr)
					c.logf("CI: querying nsqd %s", endpoint)

					var infoResp infoRespType
					err := c.client.GETV1(endpoint, &infoResp)
					if err != nil {
						lock.Lock()
						errs = append(errs, err)
						lock.Unlock()
						return
					}

					version, err := semver.Parse(infoResp.Version)
					if err != nil {
						version, _ = semver.Parse("0.0.0")
					}

					// if BroadcastAddress/HTTPPort are missing, use the values from `addr` for
					// backwards compatibility

					if infoResp.BroadcastAddress == "" {
						var p string
						infoResp.BroadcastAddress, p, _ = net.SplitHostPort(addr)
						infoResp.HTTPPort, _ = strconv.Atoi(p)
					}
					if infoResp.Hostname == "" {
						infoResp.Hostname, _, _ = net.SplitHostPort(addr)
					}

					lock.Lock()
					producers = append(producers, &Producer{
						Version:          infoResp.Version,
						VersionObj:       version,
						BroadcastAddress: infoResp.BroadcastAddress,
						Hostname:         infoResp.Hostname,
						HTTPPort:         infoResp.HTTPPort,
						TCPPort:          infoResp.TCPPort,
						Topics:           producerTopics,
					})
					lock.Unlock()

					return
				}
			}
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(nsqdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}
	if len(errs) > 0 {
		return producers, ErrList(errs)
	}
	return producers, nil
}

// GetNSQDStats 返回从指定的Producers查询到topic和chnnel的状态汇总信息
// GetNSQDStats returns aggregate topic and channel stats from the given Producers
//
// if selectedChannel is empty, this will return stats for topic/channel
// if selectedTopic is empty, this will return stats for *all* topic/channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func (c *ClusterInfo) GetNSQDStats(producers Producers, selectedTopic string, selectedChannel string) ([]*TopicStats, map[string]*ChannelStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup
	var topicStatsList TopicStatsList
	var errs []error

	channelStatsMap := make(map[string]*ChannelStats)

	type respType struct {
		Topics []*TopicStats `json:"topics"`
	}

	for _, p := range producers {
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()

			addr := p.HTTPAddress()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			if selectedTopic != "" {
				endpoint += "&topic=" + url.QueryEscape(selectedTopic)
				if selectedChannel != "" {
					endpoint += "&channel=" + url.QueryEscape(selectedChannel)
				}
			}

			c.logf("CI: querying nsqd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			for _, topic := range resp.Topics {
				topic.Node = addr
				topic.Hostname = p.Hostname
				topic.MemoryDepth = topic.Depth - topic.BackendDepth
				if selectedTopic != "" && topic.TopicName != selectedTopic {
					continue
				}
				topicStatsList = append(topicStatsList, topic)

				for _, channel := range topic.Channels {
					channel.Node = addr
					channel.Hostname = p.Hostname
					channel.TopicName = topic.TopicName
					channel.MemoryDepth = channel.Depth - channel.BackendDepth
					key := channel.ChannelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topic.TopicName, channel.ChannelName)
					}
					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							Node:        addr,
							TopicName:   topic.TopicName,
							ChannelName: channel.ChannelName,
						}
						channelStatsMap[key] = channelStats
					}
					for _, c := range channel.Clients {
						c.Node = addr
					}
					channelStats.Add(channel)
				}
			}
		}(p)
	}
	wg.Wait()

	if len(errs) == len(producers) {
		return nil, nil, fmt.Errorf("Failed to query any nsqd: %s", ErrList(errs))
	}

	sort.Sort(TopicStatsByHost{topicStatsList})

	if len(errs) > 0 {
		return topicStatsList, channelStatsMap, ErrList(errs)
	}
	return topicStatsList, channelStatsMap, nil
}

// TombstoneNodeForTopic 从给定的所有lookupd上逻辑删除生产指定topic的node
// TombstoneNodeForTopic tombstones the given node for the given topic on all the given nsqlookupd
// and deletes the topic from the node
func (c *ClusterInfo) TombstoneNodeForTopic(topic string, node string, lookupdHTTPAddrs []string) error {
	var errs []error

	// tombstone the topic on all the lookupds
	qs := fmt.Sprintf("topic=%s&node=%s", url.QueryEscape(topic), url.QueryEscape(node))
	err := c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/tombstone", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// 查询该ndqd node的信息
	producers, err := c.GetNSQDProducers([]string{node})
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// 从该nsqd生产者删除出topic
	// delete the topic on the producer
	qs = fmt.Sprintf("topic=%s", url.QueryEscape(topic))
	err = c.producersPOST(producers, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

// CreateTopicChannel 创建topic和channel
func (c *ClusterInfo) CreateTopicChannel(topicName string, channelName string, lookupdHTTPAddrs []string) error {
	var errs []error

	// 在所有的nsqlookupd上注册topic
	// create the topic on all the nsqlookupd
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	err := c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/create", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(channelName) > 0 {
		qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))

		// 在所有的nsqlookupd上注册channel
		// create the channel on all the nsqlookupd
		err := c.nsqlookupdPOST(lookupdHTTPAddrs, "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}

		// TODO: 这时候producers应该是空啊
		// 在所有生产topic的nsqd上创建channel
		// create the channel on all the nsqd that produce the topic
		producers, err := c.GetLookupdTopicProducers(topicName, lookupdHTTPAddrs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
		err = c.producersPOST(producers, "channel/create", qs)
		if err != nil {
			pe, ok := err.(PartialErr)
			if !ok {
				return err
			}
			errs = append(errs, pe.Errors()...)
		}
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

// 删除一个topic 先从nsqlookupd中取消注册 再从所有生产该topic的nsqd中删除
func (c *ClusterInfo) DeleteTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	var errs []error

	// topic将要删除了 先获取下这个topic的所有生产者
	// for topic removal, you need to get all the producers _first_
	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))

	// 把topic从所有的nsqlookupd中取消注册
	// remove the topic from all the nsqlookupd
	err = c.nsqlookupdPOST(lookupdHTTPAddrs, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// 从所有生产该topic的nsqd中删除topic
	// remove the topic from all the nsqd that produce this topic
	err = c.producersPOST(producers, "topic/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) DeleteChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	var errs []error

	// channel要删除了 先获取下这个topic的所有生产者
	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))

	// 把channel从所有的nsqlookupd中取消注册
	// remove the channel from all the nsqlookupd
	err = c.nsqlookupdPOST(lookupdHTTPAddrs, "channel/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	// 从所有生产该topic的nsqd中删除channel
	// remove the channel from all the nsqd that produce this topic
	err = c.producersPOST(producers, "channel/delete", qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

// 对topic和channel的一些操作
func (c *ClusterInfo) PauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/pause", qs)
}

func (c *ClusterInfo) UnPauseTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/unpause", qs)
}

func (c *ClusterInfo) PauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/pause", qs)
}

func (c *ClusterInfo) UnPauseChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/unpause", qs)
}

func (c *ClusterInfo) EmptyTopic(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s", url.QueryEscape(topicName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "topic/empty", qs)
}

func (c *ClusterInfo) EmptyChannel(topicName string, channelName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) error {
	qs := fmt.Sprintf("topic=%s&channel=%s", url.QueryEscape(topicName), url.QueryEscape(channelName))
	return c.actionHelper(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs, "channel/empty", qs)
}

func (c *ClusterInfo) actionHelper(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string, uri string, qs string) error {
	var errs []error

	producers, err := c.GetTopicProducers(topicName, lookupdHTTPAddrs, nsqdHTTPAddrs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	err = c.producersPOST(producers, uri, qs)
	if err != nil {
		pe, ok := err.(PartialErr)
		if !ok {
			return err
		}
		errs = append(errs, pe.Errors()...)
	}

	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

func (c *ClusterInfo) GetProducers(lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		return c.GetLookupdProducers(lookupdHTTPAddrs)
	}
	return c.GetNSQDProducers(nsqdHTTPAddrs)
}

func (c *ClusterInfo) GetTopicProducers(topicName string, lookupdHTTPAddrs []string, nsqdHTTPAddrs []string) (Producers, error) {
	if len(lookupdHTTPAddrs) != 0 {
		return c.GetLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	}
	return c.GetNSQDTopicProducers(topicName, nsqdHTTPAddrs)
}

// 向指定的nsqlookupd发送post请求
func (c *ClusterInfo) nsqlookupdPOST(addrs []string, uri string, qs string) error {
	var errs []error
	for _, addr := range addrs {
		endpoint := fmt.Sprintf("http://%s/%s?%s", addr, uri, qs)
		c.logf("CI: querying nsqlookupd %s", endpoint)
		err := c.client.POSTV1(endpoint)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}

// 向指定的nsq发送post请求
func (c *ClusterInfo) producersPOST(pl Producers, uri string, qs string) error {
	var errs []error
	for _, p := range pl {
		endpoint := fmt.Sprintf("http://%s/%s?%s", p.HTTPAddress(), uri, qs)
		c.logf("CI: querying nsqd %s", endpoint)
		err := c.client.POSTV1(endpoint)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return ErrList(errs)
	}
	return nil
}
