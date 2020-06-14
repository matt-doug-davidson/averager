package main

import "github.com/project-flogo/core/data/coerce"

// Settings for the package
type Settings struct {
	Mappings       string `md:"mappings,required"`
	OutputInterval string `md:"outputInterval,required"`
	InputInterval  int64  `md:"inputInterval,required"`
	InputOffset    int64  `md:"inputOffset,required"`
}

// Input for the package
type Input struct {
	ConnectorMsg map[string]interface{} `md:"connectorMsg"`
}

// Output for the package
type Output struct {
}

// ToMap converts from structure to a map
func (i *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"connectorMsg": i.ConnectorMsg,
	}
}

// FromMap converts fields in map to type specified in structure
func (i *Input) FromMap(values map[string]interface{}) error {
	var err error

	// Converts to string
	i.ConnectorMsg, err = coerce.ToObject(values["connectorMsg"])
	if err != nil {
		return err
	}
	return nil
}

// ToMap converts from structure to a map
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{}
}

// FromMap converts from map to whatever type .
func (o *Output) FromMap(values map[string]interface{}) error {
	return nil
}
