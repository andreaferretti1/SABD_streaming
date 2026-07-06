import os
from influxdb_client import InfluxDBClient


def init_influxdb():
    org = os.getenv("DOCKER_INFLUXDB_INIT_ORG")

    token = os.getenv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN")

    url = "http://influxdb:8086"

    # Creo il client
    influx_client = InfluxDBClient(url=url, token=token, org=org)

    # Creo i bucket
    buckets_to_create = ["q1_results", "q2_results"]
    buckets_api = influx_client.buckets_api()

    for bucket in buckets_to_create:
        existing_bucket = buckets_api.find_bucket_by_name(bucket)

        if existing_bucket is None:
            buckets_api.create_bucket(bucket_name=bucket, org=org)

    return influx_client, influx_client.write_api()