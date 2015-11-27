package graph

import (
	"crypto/md5"
	"fmt"
	cmodel "github.com/open-falcon/common/model"
	"io"
	"github.com/jdjr/drrs/golang/sdk"
	"time"
)

const (
	F_TIMEOUT = 10 // fetch timeout = 10 seconds
)

func Md5(raw string) string {
	h := md5.New()
	io.WriteString(h, raw)

	return fmt.Sprintf("%x", h.Sum(nil))
}

func RrdFileName(md5 string) string {
	return fmt.Sprintf("%s.rrd", md5)
}

func Fetch(filename string, cf string, start, end int64, addr string) ([]*cmodel.RRDData, error) {
	fetchPkg, err := sdk.Make_drrs_fetch(&filename, &cf, &start, &end, nil)
	if err != nil {
		return []*cmodel.RRDData{}, err
	}

	timeout := time.Second * time.Duration(F_TIMEOUT)
	fetchRes, err := sdk.DRRSFetch(fetchPkg, timeout, addr)
	if err != nil {
		return []*cmodel.RRDData{}, err
	}

	datas := fetchRes.GetDatas()
	values := datas[0].GetValue()
	size := len(values)
	ret := make([]*cmodel.RRDData, size)

	for i := range values {
		d := &cmodel.RRDData{
			Timestamp: values[i].GetTimestamp(),
			Value:     cmodel.JsonFloat(values[i].GetValue()),
		}
		ret[i] = d
	}

	return ret, nil
}
