<!--
title: Averager
weight: 4616
-->

# Averager
This activity allows you to set and get global App attributes.

## Installation

### Flogo CLI
```bash
flogo install github.com/matt-doug-davidson/averager
```

## Configuration

### Settings:
| Name | Type   | Description
|:---  | :---   | :---    
| inputinterval | int | The interval in minutes between input - **REQUIRED**         
| inputOffset   | int | The offset from the interval. Adds a delay to the expected arrival of inputs - **REQUIRED**
| outputInterval | string | The expected output interval. Standard values of "1H", "1D", "R8H" or "#m". The value "1H" indicates that average is taken at the top of the hour. The Value "1D" is used to average over a day and reported with the first reading of the new day. The value "R8H" is running 8 hour averaging. The "#m" is a minute interval wher # is <= 60 and 60 MOD # is 0. **REQUIRED**
| mappings| string | Mapping of input sensor names to connector field names. This is formatted as a json string **REQUIRED**

### Input:
| Name  | Type   | Description
|:---   | :---   | :---    
| connectorMsg | object |  A standard connector message


### Output:
| Name  | Type   | Description
|:---   | :---   | :---    
| connectorMsg | object |  A standard connector message


## Examples

### 

```json
{
   "id": "averager_8",
   "name": "R8H Averager",
   "description": "Averages samples over an interval",
   "activity": {
   "ref": "#averager",
   "input": {
      "connectorMsg": "=$activity[averager_5].connectorMsg"
   },
   "settings": {
      "inputInterval": 60,
      "inputOffset": 0,
      "outputInterval": "R8H",
      "mappings": {
         "mapping": {
         "NO21H": {
            "field": "NO2R8H"
         },
         "SO21H": {
            "field": "SO2R8H"
         }
         }
      }
   }
   }
}
```
