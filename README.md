# people-analytics-lib

## Owner

people-analytics-team

## Description

This libary is to simplifiy some common used functions for the people analytics team.
Please check following information:

- [version management](#version-management)
- [implementations](#implementations)

### version management

following version management should be used:

```
[major].[minor].[bugfix]
```

### implementations

- [_main_](main.py): in the function is an examples implemented
- [_connector_](./src/people_analytics_lib/connector.py): in this file the FTP and sFTP are implemented
- [_dataloader_](./src/people_analytics_lib/dataloader.py): in this file are some predefined dataloader implemented
- [_utils_](./src/people_analytics_lib/utils.py): in this file are some common used functions implemented

## Contribute

- Install dependencies

```
pip install people-analytics-lib
```

## How to use the produced library

You need to use the below pip.conf file.
**_pip.conf_**

```
[global]
index-url = https://artifactory.2b82.aws.cloud.airbus.corp/artifactory/api/pypi/pypi-airbus-virtual/simple
extra-index-url = https://artifactory.2b82.aws.cloud.airbus.corp/artifactory/api/pypi/r-airbus-pypi-local/simple
cert = path_to_airbus-certificate
trusted-host = artifactory.2b82.aws.cloud.airbus.corp
```
