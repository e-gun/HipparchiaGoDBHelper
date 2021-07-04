//    HipparchiaGoDBHelper: search and vector helper app and functions for HipparchiaServer
//    Copyright: E Gunderson 2021
//    License: GNU GENERAL PUBLIC LICENSE 3
//        (see LICENSE in the top level directory of the distribution)

package main

type PrerolledQuery struct {
	TempTable string
	PsqlQuery string
	PsqlData  string
}

type DbWorkline struct {
	WkUID       string
	TbIndex     int
	Lvl5Value   string
	Lvl4Value   string
	Lvl3Value   string
	Lvl2Value   string
	Lvl1Value   string
	Lvl0Value   string
	MarkedUp    string
	Accented    string
	Stripped    string
	Hypenated   string
	Annotations string
}

type RedisLogin struct {
	Addr     string
	Password string
	DB       int
}

type PostgresLogin struct {
	Host   string
	Port   int
	User   string
	Pass   string
	DBName string
}

// https://golangbyexample.com/sort-custom-struct-collection-golang/
type WeightedHeadword struct {
	Word  string
	Count int
}

type WHWList []WeightedHeadword

func (w WHWList) Len() int {
	return len(w)
}

func (w WHWList) Less(i, j int) bool {
	return w[i].Count > w[j].Count
}

func (w WHWList) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

type BagWithLocus struct {
	Loc string
	Bag string
}

type DbMorphology struct {
	Observed   string
	Xrefs      string
	PefixXrefs string
	RawPossib  string
}

type CompositePollingData struct {
	// this has to be kept in sync with rediskeys[8] and HipparchiaServer's interface
	Launchtime    float64
	Active        string // redis polls store 'yes' or 'no'; but the value is converted to T/F by .getactivity()
	Statusmessage string
	Remaining     int64
	Poolofwork    int64
	Hitcount      int64
	Portnumber    int64
	Notes         string
	ID            string // this is not stored in redis; it is asserted here
}

type CurrentConfiguration struct {
	RedisKey    string
	MaxHits     int64
	WorkerCount int
	LogLevel    int
	RedisInfo   string
	PosgresInfo string
	BagMethod   string
	SentPerBag  int
	VectTestDB  string
	VectStart   int
	VectEnd     int
	VSkipHW     string
	VSkipInf    string
	IsVectPtr   *bool
	IsWSPtr     *bool
	WSPort      int
	WSFail      int
	WSSave      int
	ProfCPUPtr  *bool
	ProfMemPtr  *bool
	SendVersPtr *bool
	RLogin      RedisLogin
	PGLogin     PostgresLogin
}
