package nqm

import (
	"fmt"
)

/**
 * Macro-struct re-used by various data
 */
type Metrics struct {
	Max int16 `json:"max"`
	Min int16 `json:"min"`
	Avg float32 `json:"avg"`
	Med int16 `json:"med"`
	Mdev float32 `json:"mdev"`
	Loss float32 `json:"loss"`
	Count int32 `json:"count"`
	NumberOfSentPackets uint64 `json:"number_of_sent_packets"`
	NumberOfReceivedPackets uint64 `json:"number_of_received_packets"`
}
// :~)

/**
 * ORM/JSON Models
 */

var nilProvince = (*Province)(nil)
type Province struct {
	Id int16 `orm:"pk;column(pv_id)" json:"id"`
	Name string `orm:"column(pv_name)" json:"name"`
}
func (province *Province) TableName() string {
    return "owl_province"
}
func (province *Province) getCacheKeyWithId() string {
	return fmt.Sprintf("!id!%d", province.Id)
}

var nilTarget = (*Target)(nil)
type Target struct {
	Id int32 `orm:"pk;column(tg_id)"`
	Host string `orm:"column(tg_host)"`
}
func (target *Target) TableName() string {
    return "nqm_target"
}
func (target *Target) getCacheKeyWithId() string {
	return fmt.Sprintf("!id!%d", target.Id)
}

var nilIsp = (*Isp)(nil)
type Isp struct {
	Id int16 `orm:"pk;column(isp_id)" json:"id"`
	Name string `orm:"column(isp_name)" json:"name"`
}
func (isp *Isp) TableName() string {
    return "owl_isp"
}
func (isp *Isp) getCacheKeyWithId() string {
	return fmt.Sprintf("!id!%d", isp.Id)
}

var nilCity = (*City)(nil)
type City struct {
	Id int16 `orm:"pk;column(ct_id)" json:"id"`
	Name string `orm:"column(ct_name)" json:"name"`
}
func (city *City) TableName() string {
    return "owl_city"
}
func (city *City) getCacheKeyWithId() string {
	return fmt.Sprintf("!id!%d", city.Id)
}
// :~)
