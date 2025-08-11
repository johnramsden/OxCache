# Metrics

Metrics are optionally exposed to Prometheus.

To set up a container with docker compose run:

``` 
cd docker 
docker compose up -d 
``` 

Prometheus should now be exposed at: http://127.0.0.1:9090

If this is on a remote server you can SSH tunnel with (eg cortes):

```
ssh -L 12344:127.0.0.1:9090 cortes
``` 

Then you can locally visit http://127.0.0.1:12344/query

By default this expects metrics to be exposed to the prometheus at port 9000 (set in prometheus.yml), make sure to set the appropriate port and IP address in OxCache configuration:

```toml
[metrics]
ip_addr = "127.0.0.1"
port = 9000 
``` 

Now running the server metrics should start being collected. On each individual start of the server it sets a new "run_id", set to the Unix timestamp from when the server started. This can be used to distinguish individual runs.

## Metrics

Various metrics are being exposed, and will be completed if you start typing `oxcache_`. You can use promql queries to determine throughput based on the read/written totals:

``` 
rate(oxcache_written_bytes_total[1m]) 
rate(oxcache_read_bytes_total[1m])
``` 

Time frame can be adjusted to modify the binning