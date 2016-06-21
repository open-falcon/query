package g

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/Cepave/query/logger"
	"github.com/toolkits/file"
)

type HttpConfig struct {
	Enabled bool   `json:"enabled"`
	Listen  string `json:"listen"`
}

type GinHttpConfig struct {
	Enabled bool   `json:"enabled"`
	Listen  string `json:"listen"`
}

type GraphConfig struct {
	ConnTimeout int32             `json:"connTimeout"`
	CallTimeout int32             `json:"callTimeout"`
	MaxConns    int32             `json:"maxConns"`
	MaxIdle     int32             `json:"maxIdle"`
	Replicas    int32             `json:"replicas"`
	Cluster     map[string]string `json:"cluster"`
}

type ApiConfig struct {
	Name      string `json:"name"`
	Token     string `json:"token"`
	Contact   string `json:"contact"`
	Event     string `json:"event"`
	Map       string `json:"map"`
	Geo       string `json:"geo"`
	Uplink    string `json:"uplink"`
	Query     string `json:"query"`
	Dashboard string `json:"dashboard"`
	Max       int    `json:"max"`
}

type DbConfig struct {
	Addr string `json:"addr"`
	Idle int    `json:"idle"`
	Max  int    `json:"max"`
}

type NqmLogConfig struct {
	JsonrpcUrl string `json:"jsonrpcUrl"`
}

type NqmConfig struct {
	Addr string `json:"addr"`
	Idle int    `json:"idle"`
	Max  int    `json:"max"`
}

type GrpcConfig struct {
	Enabled bool `json:"enabled"`
	Port    int  `json:"port"`
}
type GraphDB struct {
	Addr  string `json:"addr"`
	Idle  int    `json:"idle"`
	Max   int    `json:"max"`
	Limit int    `json:"limit"`
}

type GlobalConfig struct {
	Debug   bool           `json:"debug"`
	Http    *HttpConfig    `json:"http"`
	Graph   *GraphConfig   `json:"graph"`
	Api     *ApiConfig     `json:"api"`
	Db      *DbConfig      `json:"db"`
	NqmLog  *NqmLogConfig  `json:nqmlog`
	Nqm     *NqmConfig     `json:"nqm"`
	Grpc    *GrpcConfig    `json:"grpc"`
	GinHttp *GinHttpConfig `json:"gin_http"`
	GraphDB *GraphDB       `json:"graphdb"`
}

var (
	ConfigFile string
	config     *GlobalConfig
	configLock = new(sync.RWMutex)
)

// Gets the configuration
func Config() *GlobalConfig {
	configLock.RLock()
	defer configLock.RUnlock()
	return config
}

// Sets the config directly
func SetConfig(newConfig *GlobalConfig) {
	configLock.RLock()
	defer configLock.RUnlock()
	config = newConfig
}

func ParseConfig(cfg string) {
	if cfg == "" {
		log.Fatalln("config file not specified: use -c $filename")
	}

	if !file.IsExist(cfg) {
		log.Fatalln("config file specified not found:", cfg)
	}

	ConfigFile = cfg

	configContent, err := file.ToTrimString(cfg)
	if err != nil {
		log.Fatalln("read config file", cfg, "error:", err.Error())
	}

	var c GlobalConfig
	err = json.Unmarshal([]byte(configContent), &c)
	if err != nil {
		log.Fatalln("parse config file", cfg, "error:", err.Error())
	}

	SetConfig(&c)

	logger.InitLogger(c.Debug)
	log.Println("g.ParseConfig ok, file", cfg)
}
