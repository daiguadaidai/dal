package types

import (
	"encoding/json"
	"github.com/go-sql-driver/mysql"
	"time"
)

type NullTime struct {
	mysql.NullTime
}

func NewNullTime(data Time, force bool) NullTime {
	return NullTime{mysql.NullTime{time.Time(data), true}}
}

func (v *NullTime) MarshalJSON() ([]byte, error) {
	if v.Valid {
		return json.Marshal(v.Time)
	} else {
		return json.Marshal(nil)
	}
}

func (v *NullTime) UnmarshalJSON(data []byte) error {
	var s *time.Time
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s != nil {
		v.Valid = true
		v.Time = *s
	} else {
		v.Valid = false
	}
	return nil
}

const (
	TIME_FORMAT = "2006-01-02 15:04:05"
)

type Time time.Time

func NewTime(timeStr string) Time {
	now, _ := time.ParseInLocation(TIME_FORMAT, timeStr, time.Local)
	return Time(now)
}

func (t *Time) UnmarshalJSON(data []byte) (err error) {
	now, err := time.ParseInLocation(`"`+TIME_FORMAT+`"`, string(data), time.Local)
	*t = Time(now)
	return
}

func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, len(TIME_FORMAT)+2)
	b = append(b, '"')
	b = time.Time(t).AppendFormat(b, TIME_FORMAT)
	b = append(b, '"')
	return b, nil
}

func (t Time) String() string {
	return time.Time(t).Format(TIME_FORMAT)
}
