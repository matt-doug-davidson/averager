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

// var mapping_json = `{
// 	"NO2": {
// 		"field": "NO2M"
// 	},
// 	"SO2": {
// 		"field": "SO2"
// 	}
// }
// `

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
	//_ = activity.Register(&Activity{})
	_ = activity.Register(&Activity{}, New) /* Stand alone test */
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
	// // Get current times
	// seconds := time.Now().Unix()
	// // Current mark
	// currentMinute := seconds / 60                             // minutes
	// minuteOfHour := currentMinute % 60                        // minutes
	// currentMarkOfHour := (minuteOfHour / interval) * interval // minutes
	// // Next Mark
	// nextMarkOfHour := currentMarkOfHour + interval
	// nextMarkSeconds := currentMinute + nextMarkOfHour - minuteOfHour
	nextMarkNanoSecs := timestamps.NextMinuteTimestamp(interval)
	nanoSeconds := time.Now().UnixNano()
	delayToNextMark := nextMarkNanoSecs - nanoSeconds
	d := time.Duration(delayToNextMark) * time.Nanosecond
	return d
}

func (a *Activity) getDelay() time.Duration {
	var delay time.Duration = time.Duration(60) * time.Second
	fmt.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
	fmt.Println(a.OutputInterval)
	if a.OutputInterval == "1H" {
		delay = NextHourDelay()
	} else if a.OutputInterval == "1D" {
		delay = NextDayDelay()
	} else if a.OutputInterval == "R8H" {
		delay = NextHourDelay()
	} else {
		interval := strings.Split(a.OutputInterval, "m")
		fmt.Println(interval)
		fmt.Println(len(interval))
		if interval[1] != "" {
			fmt.Println("Not in minutes but should be")
		}
		intervalI, err := strconv.ParseInt(interval[0], 10, 64)
		if err != nil {
			fmt.Println("Eror: ", err.Error())

		}
		fmt.Println(intervalI)
		delay = NextMinuteMark(intervalI)
	}
	fmt.Println(delay)
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
	a.Accumulators[sensor] = nil
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
	fmt.Printf("%T\n", value)
	fmt.Println(value)
	return value
}

// Eval evaluates the activity
func (a *Activity) Eval(ctx activity.Context) (done bool, err error) { /* Standalone Test */
	//func (a *Activity) Eval(ConnectorMsg map[string]interface{}) (done bool, err error) {
	logger := ctx.Logger()
	logger.Info("averager:Eval enter")

	input := &Input{}
	err = ctx.GetInputObject(input)
	if err != nil {
		logger.Error("Failed to input object")
		return false, err
	}
	//fmt.Println(input.ConnectorMsg)
	//fmt.Println(input.ConnectorMsg["entity"])
	fmt.Println("\n\n\n+++++++++++++++++++++++++++Eval++++++++++++++++++++++++: \n", input.ConnectorMsg)
	entity := input.ConnectorMsg["entity"].(string)
	payload := input.ConnectorMsg["data"].(map[string]interface{})
	fmt.Println("payload\n", payload)
	//fmt.Println("datetime\n", payload["datetime"])
	rcvdTs := timestamps.UTCZToUTCTimestamp(payload["datetime"].(string))
	// fmt.Println("-------------")
	// fmt.Println(rcvdTs)
	// fmt.Println(timestamps.TimestampToLocalTimestring(rcvdTs))
	// fmt.Println(timestamps.TimestampToLocalTimestring(a.TargetTimestamp))
	// fmt.Println("-------------")
	//fmt.Println("values\n", payload["values"])

	values := payload["values"].([]map[string]interface{})
	//fmt.Println("values\n", values)
	datetime := ""
	reportValues := []map[string]interface{}{}
	fmt.Println("\n", a, "\n")
	fmt.Println("rcvdTs     ", timestamps.TimestampToLocalTimestring(rcvdTs))
	fmt.Println("Min Margin ", timestamps.TimestampToLocalTimestring(a.TargetTimestamp-a.Margins))
	fmt.Println("Mid Margin ", timestamps.TimestampToLocalTimestring(a.TargetTimestamp))
	fmt.Println("Max Margin ", timestamps.TimestampToLocalTimestring(a.TargetTimestamp+a.Margins))

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
				fmt.Println("Found::::")
				a.append(sensor, amount)
				fmt.Println(a.Accumulators[sensor])
				if int64(len(a.Accumulators[sensor])) >= a.AccumulatorLength/2 {
					value["field"] = a.Sensors[sensor]
					value["amount"] = a.average(sensor)
					fmt.Println("")
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
	if len(reportValues) != 0 {
		// Create a message an put it in the output
		data := atif.EncodeData(datetime, reportValues)
		output := atif.EncodeOutput(entity, data)
		fmt.Println(output)
		// ConnectorMessage := make(map[string]interface{})
		// ConnectorMessage["msg"] = connectorData

		err = ctx.SetOutput("connectorMsg", output)
		if err != nil {
			logger.Error("Failed to set output oject ", err.Error())
			return false, err
		}
	}
	fmt.Println("reportValues:\n", reportValues, "\n")
	fmt.Println(a)
	return true, nil
}

func (a *Activity) setNextTargetTimestamp() {
	a.TargetTimestamp = a.getNextTargetTimestamp()
	fmt.Println("%%%%%%%%\n", a.TargetTimestamp, "\n%%%%%%%")

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
	fmt.Println("-------------------- atTimeMark (strart) ------------------")
	currentTimestamp := timestamps.UTCTimestamp()
	fmt.Println(currentTimestamp)

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
	fmt.Println(">>>a.TargetTimestamp ", a.TargetTimestamp)
	fmt.Println(">>> tSpan ", tSpan)
	previousTarget := a.TargetTimestamp - tSpan
	fmt.Println(">>> previousarget ", previousTarget)
	upperMargin := previousTarget + a.Margins
	fmt.Println(">>> uppserMargin ", upperMargin)
	lowerMargin := previousTarget - a.Margins
	fmt.Println(">>> lowerMargin ", lowerMargin)
	var rc bool
	if currentTimestamp > lowerMargin && currentTimestamp < upperMargin {
		fmt.Println("This is a previous target")
		rc = true
	} else {
		fmt.Println("This is a not previous target")
		rc = false
	}

	fmt.Println("-------------------- atTimeMark (end) ------------------")
	return rc
}

func New(ctx activity.InitContext) (activity.Activity, error) { /* Standalone test */
	//func New(ctx activity.InitContext) (Activity, error) {

	logger := ctx.Logger()
	logger.Info("averager:New enter")
	s := &Settings{}
	err := metadata.MapToStruct(ctx.Settings(), s, true)
	if err != nil {
		logger.Error("Failed to convert settings")
		return nil, err
	}
	fmt.Println("s ", s)
	/* Debug */
	// s.OutputInterval = "1H"
	// s.InputInterval = 10
	// s.InputOffset = 2
	Mappings := map[string]interface{}{}
	// s.Mappings = mapping_json
	/* Debug */
	fmt.Println("s.Mappings ", s.Mappings)
	json.Unmarshal([]byte(s.Mappings), &Mappings)

	sensors := make(map[string]string)
	averagers := make(map[string][]float64)
	fmt.Println(Mappings)
	for i, v := range Mappings {
		fmt.Println("i, v ", i, v)
		vv := v.(map[string]interface{})
		fmt.Println("vv ", vv)
		f, found := vv["field"]
		fmt.Println("f, found ", f, found)
		if !found {
			continue
		}
		sensors[i] = vv["field"].(string)
		averagers[i] = make([]float64, 0, 0)
		fmt.Println(f)
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
		fmt.Println("minutes ", minutes)
		accumLength = (minutes / s.InputInterval) + 1
	}

	margins := timestamps.Nanoseconds(float64(s.InputInterval) / 2.0)
	fmt.Println("margins: ", margins)

	act := &Activity{
		/* act := Activity{ Standalone Test */
		Sensors:           sensors,
		Accumulators:      averagers,
		AccumulatorLength: accumLength,
		InputOffset:       s.InputOffset,
		OutputInterval:    s.OutputInterval,
		Margins:           margins,
		Entity:            ""}
	act.setNextTargetTimestamp()
	fmt.Println(&act)
	fmt.Println(act.TargetTimestamp)

	fmt.Println("------ TargetTimestamp-------")
	fmt.Println(timestamps.TimestampToLocalTimestring(act.TargetTimestamp))
	fmt.Println("-------------")

	return act, nil
}

// func main() {
// 	fmt.Println("Starting")

// 	iCtx := test.NewActivityInitContext(nil, nil)
// 	act, _ := New(iCtx)

// 	fmt.Println(act)

// 	values := []map[string]interface{}{}
// 	value := make(map[string]interface{})
// 	value["field"] = "NO2"
// 	value["amount"] = 5.0
// 	values = append(values, value)
// 	fmt.Println("****** ", values)
// 	value1 := map[string]interface{}{}
// 	value1["field"] = "SO2"
// 	value1["amount"] = 6.7
// 	fmt.Println("****** ", values)
// 	values = append(values, value1)
// 	fmt.Println("******** ", values)

// 	timestamp := int(time.Now().Unix())
// 	timestamp *= 1000 // Convert to milliseconds
// 	timestampStr := strconv.Itoa(timestamp)
// 	msts := timestamps.MillisecondTimestamp{}
// 	datetime := msts.ConvertUTCZ(timestampStr)

// 	message := map[string]interface{}{}
// 	message["values"] = values
// 	message["datetime"] = datetime
// 	message["messageId"] = uuid.New().String()
// 	fmt.Println(message)

// 	output := map[string]interface{}{}
// 	output["data"] = message
// 	output["entity"] = "/A/B/C"

// 	fmt.Println(output)
// 	//act.Eval(output)
// 	fmt.Println(act)

// 	a := func() {
// 		fmt.Println("---------------------###############")
// 		values := []map[string]interface{}{}
// 		value := make(map[string]interface{})
// 		value["field"] = "NO2"
// 		value["amount"] = rand.Float64() * 10
// 		values = append(values, value)
// 		//fmt.Println("****** ", values)
// 		value1 := map[string]interface{}{}
// 		value1["field"] = "SO2"
// 		value1["amount"] = rand.Float64() * 10
// 		//fmt.Println("****** ", values)
// 		values = append(values, value1)
// 		//fmt.Println("******** ", values)
// 		timestamp := int(time.Now().Unix())
// 		timestamp *= 1000 // Convert to milliseconds
// 		timestampStr := strconv.Itoa(timestamp)
// 		msts := timestamps.MillisecondTimestamp{}
// 		datetime := msts.ConvertUTCZ(timestampStr)

// 		message := map[string]interface{}{}
// 		message["values"] = values
// 		message["datetime"] = datetime
// 		message["messageId"] = uuid.New().String()

// 		output := map[string]interface{}{}
// 		output["data"] = message
// 		output["entity"] = "/A/B/C"

// 		//fmt.Println("output:\n", output)
// 		act.Eval(output)
// 	}
// 	for j := 0; j < 2; j++ {
// 		d := NextMinuteMark(10) + time.Duration(2)*time.Minute
// 		time.Sleep(d)
// 		a()
// 	}
// 	for j := 0; j < 2; j++ {
// 		d := NextMinuteMark(10) + time.Duration(2)*time.Minute
// 		time.Sleep(d)

// 		a()

// 	}
// 	for j := 0; j < 6; j++ {
// 		d := NextMinuteMark(10) + time.Duration(2)*time.Minute
// 		time.Sleep(d)
// 		a()
// 	}

// }
