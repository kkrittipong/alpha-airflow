#!/usr/bin/env bash
airflow db init
airflow webserver
airflow users --create -e krittipong@ava.fund -f Krittipong -l Kanchanapiboon -p avaalpha -r Admin -u yort