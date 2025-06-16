package helpers

import (
	"strconv"
	"strings"
	"time"
)

func TryParseTimeUTC(ts string) (time.Time, error) {
	t, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(ts))
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func TryParseFloat(ts string) (float64, error) {
	t, err := strconv.ParseFloat(ts, 32)
	if err != nil {
		return 0.0, err
	}
	return t, nil
}

func TryParseInt(ts string) (int64, error) {
	i, err := strconv.ParseInt(ts, 10, 32)
	if err != nil {
		return 0, err
	}

	return i, nil
}
