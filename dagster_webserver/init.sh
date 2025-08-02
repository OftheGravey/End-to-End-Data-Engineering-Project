#!/bin/bash
dagster-daemon run &
dagster-webserver -h '0.0.0.0' -p 3000 -w workspace.yaml 