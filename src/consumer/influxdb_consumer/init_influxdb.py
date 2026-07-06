import os
from influxdb_client import InfluxDBClient


def init_influxdb():
    org = os.getenv("DOCKER_INFLUXDB_INIT_ORG")

    token = os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN")

    url = "http://influxdb:8086"

    # Creo il client
    influx_client = InfluxDBClient(url=url, token=token, org=org)

    # Creo i bucket
    buckets_api = influx_client.buckets_api()
    buckets_api.create_bucket(bucket_name="q1_results", org=org)
    buckets_api.create_bucket(bucket_name="q2_results", org=org)

    return influx_client.write_api()