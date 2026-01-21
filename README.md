# OC-P12

## POC Architecture CDC

**Table of contents**

- [Getting started](#getting-started)
	- [(Optionnal) Download Python > 3.11](#Download-Python->-3.11)
	- [Download Docker](#download-docker)
	- [Launch services](#launch-services)
	- [Create sport events](#create-sport-events)
	- [Consult BI reports](#consult-BI-reports)
- [Architecture](#architecture)

## Getting started

### Download Python > 3.11
Download python 3.11 or higher on https://www.python.org/downloads/

### Download Power BI Desktop
Download Power BI Desktop on https://www.microsoft.com/fr-fr/download/details.aspx?id=58494

### Download Docker
Download the Docker Desktop matching your OS on https://www.docker.com/products/docker-desktop/

### Launch services
Default :
```bash
    docker compose up -d
```

### Create sport events
Default :
```bash
	python .\py\create_sport_events.py -e {nb_of_events_to_create}
```

### Consult BI reports
Open BI.pbix and reconnect sources if necessary.

## Architecture
<img src="./img/architecture.png" alt="architecture" width="600">