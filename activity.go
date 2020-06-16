package averager

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matt-doug-davidson/atif"
	"github.com/matt-doug-davidson/timestamps"
	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
)

type Activity struct {
	settings          *Settings            // Defind in metadata.go in this package
	Entity            string               // Value from the Eval message
	Sensors           map[string]string    // key: sensor string value: field
	InputOffset       int64                // Seconds
	OutputInterval    string               // "1H", "1D", "R8R", "#m" where # is minutes where 60%# is 0.
	Accumulators      map[string][]float64 // key: sensor string value: amount
	AccumulatorLength int64                // key: sensor string value: accumulator length
	TargetTimestamp   int64
	Margins           int64 // Nanoseconds
}

// Metadata returns the activity's metadata
// Common function
func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

// The init function is executed after the package is imported. This function
// runs before any other in the package.
func init() {
	_ = activity.Register(&Activity{}, New) /* Stand alone test */
}

func (a *Activity) print() {
	fmt.Println("Entity            ", a.Entity)
	fmt.Println("Sensors:")
	for k := range a.Sensors {
		fmt.Println("  ", k, "->", a.Sensors[k])
	}
	fmt.Println("InputOffset       ", a.InputOffset)
	fmt.Println("OutputInterval    ", a.OutputInterval)
	fmt.Println("TargetTimestamp   ", a.TargetTimestamp)
	fmt.Println("TargetTimestamp   ", timestamps.TimestampToLocalTimestring(a.TargetTimestamp))
	fmt.Println("AccumulatorLength ", a.AccumulatorLength)
	fmt.Println("Accumulators:")
	for k := range a.Sensors {
		fmt.Println("  ", k, ":", a.Accumulators[k])
	}
	fmt.Println("Margins           ", a.Margins)
}

// Used when the init function is called. The settings, Input and Output
// structures are optional depends application. These structures are
// defined in the metadata.go file in this package.
var activityMd = activity.ToMetadata(&Settings{}, &Input{}, &Output{})

// NextHourDelay gets the delay until the top of the next hour. The intent
// is to delay to the top of the hour.
func NextHourDelay() time.Duration {
	now := time.Now().UnixNano()
	hour := int64(60 * 60 * 1000000000)
	thisHour := (now / int64(hour))
	nextHour := thisHour + 1
	nextHourNs := nextHour * hour
	sleepTime := nextHourNs - now
	d := time.Duration(sleepTime) * time.Nanosecond
	return d
}

func NextHourTimestamp() int64 {
	now := time.Now().UnixNano()
	hour := int64(60 * 60 * 1000000000)
	thisHour := (now / int64(hour))
	nextHour := thisHour + 1
	nextHourNs := nextHour * hour
	return nextHourNs
}

// NextDayDelay gets the delay until the top of the next day. The intent
// is to delay to the top of the day. That is 00:00:00 of the next day.
func NextDayDelay() time.Duration {
	now := time.Now()
	dateT := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	future := dateT.AddDate(0, 0, 1)
	return future.Sub(now)
}

func NextDayTimestamp() int64 {
	now := time.Now()
	dateT := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	future := dateT.AddDate(0, 0, 1)
	timestamp := future.UnixNano()
	return timestamp
}

// NextMinuteMark creates a delay based upon hitting the next minute
// mark within the hour. The offset is seconds to delay after the
// minute mark.
func NextMinuteMark(interval int64) time.Duration {
	nextMarkNanoSecs := timestamps.NextMinuteTimestamp(interval)
	nanoSeconds := time.Now().UnixNano()
	delayToNextMark := nextMarkNanoSecs - nanoSeconds
	d := time.Duration(delayToNextMark) * time.Nanosecond
	return d
}

func (a *Activity) getDelay() time.Duration {
	var delay time.Duration = time.Duration(60) * time.Second

	if a.OutputInterval == "1H" {
		delay = NextHourDelay()
	} else if a.OutputInterval == "1D" {
		delay = NextDayDelay()
	} else if a.OutputInterval == "R8H" {
		delay = NextHourDelay()
	} else {
		interval := strings.Split(a.OutputInterval, "m")
		if interval[1] != "" {
			fmt.Println("Not in minutes but should be")
		}
		intervalI, err := strconv.ParseInt(interval[0], 10, 64)
		if err != nil {
			fmt.Println("Eror: ", err.Error())

		}
		delay = NextMinuteMark(intervalI)
	}
	offsetDelay := time.Duration(a.InputOffset) * time.Second
	delay += offsetDelay
	return delay
}

func (a *Activity) average(sensor string) float64 {
	sum := 0.0
	// Lock
	count := float64(len(a.Accumulators[sensor]))
	for _, v := range a.Accumulators[sensor] {
		sum += v
	}
	// Unlock
	average := sum / count
	return average
}

func (a *Activity) clear(sensor string) {
	// Do not clear the R8H accumulator
	if a.OutputInterval != "R8H" {
		a.Accumulators[sensor] = nil
	}
}

func (a *Activity) append(sensor string, amount float64) {
	a.Accumulators[sensor] = append(a.Accumulators[sensor], amount)
	if int64(len(a.Accumulators[sensor])) > a.AccumulatorLength {
		a.Accumulators[sensor] = append(a.Accumulators[sensor][:0], a.Accumulators[sensor][0+1:]...)
	}
}

// Use to adjust precision of number
var percisions map[int]float64 = map[int]float64{0: 0, 1: 10, 2: 100, 3: 1000, 4: 10000, 5: 100000}

func setPercision(v float64, digits int) float64 {
	periciseness := percisions[digits]
	value := math.Round(v*periciseness) / periciseness
	return value
}

// Eval evaluates the activity
func (a *Activity) Eval(ctx activity.Context) (done bool, err error) {
	logger := ctx.Logger()
	logger.Info("averager:Eval enter")

	input := &Input{}
	err = ctx.GetInputObject(input)
	if err != nil {
		logger.Error("Failed to input object")
		return false, err
	}

	fmt.Println("\n\n\n+++++++++++++++++++++++++++Eval++++++++++++++++++++++++: \n", input.ConnectorMsg)
	entity := input.ConnectorMsg["entity"].(string)
	payload := input.ConnectorMsg["data"].(map[string]interface{})
	//rcvdTs := timestamps.UTCZToUTCTimestamp(payload["datetime"].(string))
	// Do not use the datetime in the recieved message as it might be when a device read it. Instead
	rcvdTs := timestamps.UTCTimestamp()
	values := payload["values"].([]map[string]interface{})
	datetime := ""
	reportValues := []map[string]interface{}{}
	a.print()

	fmt.Println("Rxvd Ts ", timestamps.TimestampToLocalTimestring(rcvdTs))
	fmt.Println("Lower Margin: ", timestamps.TimestampToLocalTimestring(a.TargetTimestamp-a.Margins))
	fmt.Println("Upper Margin: ", timestamps.TimestampToLocalTimestring(a.TargetTimestamp+a.Margins))

	if rcvdTs > a.TargetTimestamp-a.Margins && rcvdTs < a.TargetTimestamp+a.Margins {
		fmt.Println("===== On time ====-")
		datetime = payload["datetime"].(string)
		// Append new value, average store average, clear accumulator
		// Note: it would be slightly faster to loop over the sensors being monitored and check
		//       for their existance in the message.
		for _, v := range values {
			sensor := v["field"].(string)
			amount := v["amount"].(float64)
			value := map[string]interface{}{}
			_, found := a.Accumulators[sensor]
			if found {
				a.append(sensor, amount)
				if int64(len(a.Accumulators[sensor])) >= a.AccumulatorLength/2 {
					value["field"] = a.Sensors[sensor]
					value["amount"] = a.average(sensor)
					reportValues = append(reportValues, value)
				}
				a.clear(sensor)
			}
		}
		a.setNextTargetTimestamp()
	} else if rcvdTs >= a.TargetTimestamp+a.Margins {
		fmt.Println("===== Late ====")
		// Need process the missed time
		// Average existing values
		// Report with target timestamps
		// Clear
		// Do not report the one reading (just received) for this interval.
		datetime = timestamps.TimestampToUTCZTimestring(a.TargetTimestamp)
		a.setNextTargetTimestamp()
		for _, v := range values {
			sensor := v["field"].(string)
			amount := v["amount"].(float64)
			value := map[string]interface{}{}
			_, found := a.Accumulators[sensor]
			if found {
				// Add averaged sensor reading to values
				if int64(len(a.Accumulators[sensor])) >= a.AccumulatorLength/2 {
					value["field"] = a.Sensors[sensor]
					value["amount"] = a.average(sensor)
					reportValues = append(reportValues, value)
				}
				// Clear this sensor's reading
				a.clear(sensor)
				// Append new sensor reading if not at the time mark
				if !a.atTimeMark() {
					a.append(sensor, amount)
				}
			}
		}
	} else if rcvdTs <= a.TargetTimestamp+a.Margins {
		// Do not adjust the next target timestamp at this point.
		fmt.Println("OK - Process accumulation")
		// Append
		for _, v := range values {
			sensor := v["field"].(string)
			amount := v["amount"].(float64)
			_, found := a.Accumulators[sensor]
			if found {
				a.append(sensor, amount)
			}
		}
	}
	rc := false
	if len(reportValues) != 0 {
		// Create a message an put it in the output
		data := atif.EncodeData(datetime, reportValues)
		output := atif.EncodeOutput(entity, data)
		fmt.Println(output)

		err = ctx.SetOutput("connectorMsg", output)
		if err != nil {
			logger.Error("Failed to set output oject ", err.Error())
			return false, err
		}
		rc = true
	}

	a.print()
	return rc, nil
}

func (a *Activity) setNextTargetTimestamp() {
	a.TargetTimestamp = a.getNextTargetTimestamp()
}

func (a *Activity) getNextTargetTimestamp() int64 {
	var ts int64
	if a.OutputInterval == "1H" || a.OutputInterval == "R8H" {
		ts = timestamps.NextHourTimestamp()
	} else if a.OutputInterval == "1D" {
		ts = timestamps.NextDayTimestamp()
	} else {
		mins := strings.Split(a.OutputInterval, "m")
		minutesInt, err := strconv.ParseInt(mins[0], 10, 64)
		if err != nil {
			ts = timestamps.NextDayTimestamp()
		} else {
			ts = timestamps.NextMinuteTimestamp(minutesInt)
		}
	}
	fmt.Println("%%%%%%%%\n", ts, "\n%%%%%%%")
	ts += timestamps.Nanoseconds(float64(a.InputOffset) * 60) // InputOffset is seconds. Convert to minutes
	return ts
}

func (a *Activity) atTimeMark() bool {
	currentTimestamp := timestamps.UTCTimestamp()

	var tSpan int64
	if a.OutputInterval == "1H" || a.OutputInterval == "R8H" {
		tSpan = timestamps.Nanoseconds(60 * 60)
	} else if a.OutputInterval == "1D" {
		tSpan = timestamps.Nanoseconds(60 * 60 * 24)
	} else {
		mins := strings.Split(a.OutputInterval, "m")
		minutesInt, err := strconv.ParseInt(mins[0], 10, 64)
		if err != nil {
			tSpan = timestamps.Nanoseconds(60 * 60 * 24)
		} else {
			tSpan = timestamps.Nanoseconds(60 * float64(minutesInt))
		}
	}
	previousTarget := a.TargetTimestamp - tSpan
	upperMargin := previousTarget + a.Margins
	lowerMargin := previousTarget - a.Margins
	var rc bool
	if currentTimestamp > lowerMargin && currentTimestamp < upperMargin {
		rc = true
	} else {
		rc = false
	}

	return rc
}

func New(ctx activity.InitContext) (activity.Activity, error) {

	logger := ctx.Logger()
	logger.Info("averager:New enter")
	s := &Settings{}
	err := metadata.MapToStruct(ctx.Settings(), s, true)
	if err != nil {
		logger.Error("Failed to convert settings")
		return nil, err
	}

	Mappings := map[string]interface{}{}

	/* Debug */
	fmt.Println("s.Mappings ", s.Mappings)
	json.Unmarshal([]byte(s.Mappings), &Mappings)
	fmt.Println("Mappings: ", Mappings)
	maps := Mappings["mapping"].(map[string]interface{})
	sensors := make(map[string]string)
	averagers := make(map[string][]float64)
	fmt.Println(maps)
	for i, v := range maps {
		vv := v.(map[string]interface{})
		_, found := vv["field"]
		if !found {
			continue
		}
		sensors[i] = vv["field"].(string)
		averagers[i] = make([]float64, 0, 0)

	}
	/*
		Create a report time stamp based on mark time. Whenever incomeing timestamp is aboe that report
		and reset the timestamp.
	*/
	/* Get the next hour and day time stamp. Add input offset */
	var accumLength int64
	if s.OutputInterval == "R8H" {
		accumLength = 8

	} else {
		var minutes int64
		if s.OutputInterval == "1H" {
			minutes = 60
		} else if s.OutputInterval == "1D" {
			minutes = 60 * 24
		} else {
			mins := strings.Split(s.OutputInterval, "m")
			minutesInt, err := strconv.ParseInt(mins[0], 10, 64)
			if err != nil {
				minutes = 200
			} else {
				minutes = minutesInt
			}
		}

		accumLength = (minutes / s.InputInterval) + 1
	}

	margins := timestamps.Nanoseconds(float64(s.InputInterval) / 2.0)

	act := &Activity{
		Sensors:           sensors,
		Accumulators:      averagers,
		AccumulatorLength: accumLength,
		InputOffset:       s.InputOffset,
		OutputInterval:    s.OutputInterval,
		Margins:           margins,
		Entity:            ""}
	act.setNextTargetTimestamp()

	return act, nil
}
