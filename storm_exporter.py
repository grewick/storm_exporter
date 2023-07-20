#!/usr/bin/python3

import time
import sys
import requests
import traceback
from prometheus_client import start_http_server, Gauge

#TOPOLOGY/SUMMARY METRICS
STORM_TOPOLOGY_UPTIME_SECONDS = Gauge('uptime_seconds','Shows how long the topology is running in seconds',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_TASKS_TOTAL = Gauge('tasks_total','Total number of tasks for this topology',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_WORKERS_TOTAL = Gauge('workers_total','Number of workers used for this topology',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_EXECUTORS_TOTAL = Gauge('executors_total','Number of executors used for this topology',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_REPLICATION_COUNT = Gauge('replication_count','Number of nimbus hosts on which this topology code is replicated',['TopologyName', 'TopologyId', 'TopologyStatus'])

STORM_TOPOLOGY_REQUESTED_MEM_ON_HEAP = Gauge('requested_mem_on_heap','Requested On-Heap Memory by User (MB)',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_REQUESTED_MEM_OFF_HEAP = Gauge('requested_mem_off_heap','Requested Off-Heap Memory by User (MB)',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_REQUESTED_TOTAL_MEM = Gauge('requested_total_mem','Requested Total Memory by User (MB)',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_REQUESTED_CPU = Gauge('requested_cpu','Requested CPU by User (%)',['TopologyName', 'TopologyId', 'TopologyStatus'])

STORM_TOPOLOGY_ASSIGNED_MEM_ON_HEAP = Gauge('assigned_mem_on_heap','Assigned On-Heap Memory by Scheduler (MB)',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_ASSIGNED_MEM_OFF_HEAP = Gauge('assigned_mem_off_heap','Assigned Off-Heap Memory by Scheduler (MB)',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_ASSIGNED_TOTAL_MEM = Gauge('assigned_total_mem','Assigned Total Memory by Scheduler (MB)',['TopologyName', 'TopologyId', 'TopologyStatus'])
STORM_TOPOLOGY_ASSIGNED_CPU = Gauge('assigned_cpu','Assigned CPU by Scheduler (%)',['TopologyName', 'TopologyId', 'TopologyStatus'])


#TOPOLOGY/STATS METRICS:
TOPOLOGY_STATS_ACKED = Gauge('topology_stats_acked','Number of messages acked in given window',['TopologyName', 'TopologyId', 'TopologyStatus', 'window'])
TOPOLOGY_STATS_FAILED = Gauge('topology_stats_failed','Number of messages failed in given window',['TopologyName', 'TopologyId', 'TopologyStatus', 'window'])
TOPOLOGY_STATS_EMITTED = Gauge('topology_stats_emitted','Number of messages emitted in given window',['TopologyName', 'TopologyId', 'TopologyStatus', 'window'])
TOPOLOGY_STATS_TRANSFERRED = Gauge('topology_stats_transferred','Number messages transferred in given window',['TopologyName', 'TopologyId', 'TopologyStatus', 'window'])

TOPOLOGY_STATS_COMPLETE_LATENCY = Gauge('topology_stats_complete_latency','Total latency for processing the message',['TopologyName', 'TopologyId', 'TopologyStatus', 'window'])


#TOPOLOGY/ID SPOUT METRICS:
STORM_TOPOLOGY_SPOUTS_EXECUTORS = Gauge('spouts_executors','Number of executors for the spout',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])
STORM_TOPOLOGY_SPOUTS_TASKS = Gauge('spouts_tasks','Total number of tasks for the spout',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])

STORM_TOPOLOGY_SPOUTS_ACKED = Gauge('spouts_acked','Number of messages acked',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])
STORM_TOPOLOGY_SPOUTS_FAILED = Gauge('spouts_failed','Number of messages failed',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])
STORM_TOPOLOGY_SPOUTS_EMITTED = Gauge('spouts_emitted','Number of messages emitted in given window',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])
STORM_TOPOLOGY_SPOUTS_TRANSFERRED = Gauge('spouts_transferred','Total number of messages transferred in given window',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])
STORM_TOPOLOGY_SPOUTS_LATEST_ERROR_EPOCH = Gauge('spouts_latest_error_epoch','Epoch (in millis) when the latest error was encountered by the spout',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])

STORM_TOPOLOGY_SPOUTS_COMPLETE_LATENCY = Gauge('spouts_complete_latency','Total latency for processing the message',['TopologyName', 'TopologyId', 'TopologyStatus', 'SpoutId'])


#TOPOLOGY/ID BOLT METRICS:
STORM_TOPOLOGY_BOLTS_CAPACITY = Gauge('bolts_capacity','This value indicates number of messages executed * average execute latency / time window',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])
STORM_TOPOLOGY_BOLTS_EXECUTORS = Gauge('bolts_executors','Number of executor tasks in the bolt component',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])
STORM_TOPOLOGY_BOLTS_TASKS = Gauge('bolts_tasks','Number of instances of bolt',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])

STORM_TOPOLOGY_BOLTS_ACKED = Gauge('bolts_acked','Number of tuples acked by the bolt',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])
STORM_TOPOLOGY_BOLTS_FAILED = Gauge('bolts_failed','Number of tuples failed by the bolt',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])
STORM_TOPOLOGY_BOLTS_EMITTED = Gauge('bolts_emitted','Number of tuples emitted by the bolt',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])
STORM_TOPOLOGY_BOLTS_TRANSFERRED = Gauge('bolts_transferred','Total number of messages transferred in given window by the bolt',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])
STORM_TOPOLOGY_BOLTS_LATEST_ERROR_EPOCH = Gauge('bolts_latest_error_epoch','Epoch (in millis) when the latest error was encountered by the bolt',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])

STORM_TOPOLOGY_BOLTS_PROCESS_LATENCY = Gauge('bolts_process_latency','Average time of the bolt to ack a message after it was received',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])
STORM_TOPOLOGY_BOLTS_EXECUTE_LATENCY = Gauge('bolts_execute_latency','Average time to run the execute method of the bolt',['TopologyName', 'TopologyId', 'TopologyStatus', 'BoltId'])


def clearGauges(gauges, labels):
    for gauge in gauges:
        try:
            gauge.remove(*labels)
        except:
            pass


def getMetric(metric):
    if metric == None:
        return 0
    else:
        return metric


def statsMetric(stat,tn,tid,tstatus):
    wd = stat['window']
    TOPOLOGY_STATS_ACKED.labels(tn,tid,tstatus,wd).set(getMetric(stat['acked']))
    TOPOLOGY_STATS_FAILED.labels(tn,tid,tstatus,wd).set(getMetric(stat['failed']))
    TOPOLOGY_STATS_EMITTED.labels(tn,tid,tstatus,wd).set(getMetric(stat['emitted']))
    TOPOLOGY_STATS_TRANSFERRED.labels(tn,tid,tstatus,wd).set(getMetric(stat['transferred']))

    TOPOLOGY_STATS_COMPLETE_LATENCY.labels(tn,tid,tstatus,wd).set(getMetric(stat['completeLatency']))


def spoutMetric(spout,tn,tid,tstatus):
    sid = spout['spoutId']
    STORM_TOPOLOGY_SPOUTS_EXECUTORS.labels(tn,tid,tstatus,sid).set(getMetric(spout['executors']))
    STORM_TOPOLOGY_SPOUTS_TASKS.labels(tn,tid,tstatus,sid).set(getMetric(spout['tasks']))

    STORM_TOPOLOGY_SPOUTS_ACKED.labels(tn,tid,tstatus,sid).set(getMetric(spout['acked']))
    STORM_TOPOLOGY_SPOUTS_FAILED.labels(tn,tid,tstatus,sid).set(getMetric(spout['failed']))
    STORM_TOPOLOGY_SPOUTS_EMITTED.labels(tn,tid,tstatus,sid).set(getMetric(spout['emitted']))
    STORM_TOPOLOGY_SPOUTS_TRANSFERRED.labels(tn,tid,tstatus,sid).set(getMetric(spout['transferred']))
    STORM_TOPOLOGY_SPOUTS_LATEST_ERROR_EPOCH.labels(tn,tid,tstatus,sid).set(getMetric(spout['errorTime']))

    STORM_TOPOLOGY_SPOUTS_COMPLETE_LATENCY.labels(tn,tid,tstatus,sid).set(getMetric(spout['completeLatency']))


def boltMetric(bolt,tn,tid,tstatus):
    bid = bolt['boltId']

    STORM_TOPOLOGY_BOLTS_CAPACITY.labels(tn,tid,tstatus,bid).set(getMetric(bolt['capacity']))
    STORM_TOPOLOGY_BOLTS_EXECUTORS.labels(tn,tid,tstatus,bid).set(getMetric(bolt['executors']))
    STORM_TOPOLOGY_BOLTS_TASKS.labels(tn,tid,tstatus,bid).set(getMetric(bolt['tasks']))

    STORM_TOPOLOGY_BOLTS_ACKED.labels(tn,tid,tstatus,bid).set(getMetric(bolt['acked']))
    STORM_TOPOLOGY_BOLTS_FAILED.labels(tn,tid,tstatus,bid).set(getMetric(bolt['failed']))
    STORM_TOPOLOGY_BOLTS_EMITTED.labels(tn,tid,tstatus,bid).set(getMetric(bolt['emitted']))
    STORM_TOPOLOGY_BOLTS_TRANSFERRED.labels(tn,tid,tstatus,bid).set(getMetric(bolt['transferred']))
    STORM_TOPOLOGY_BOLTS_LATEST_ERROR_EPOCH.labels(tn,tid,tstatus,bid).set(getMetric(bolt['errorTime']))

    STORM_TOPOLOGY_BOLTS_EXECUTE_LATENCY.labels(tn,tid,tstatus,bid).set(getMetric(bolt['executeLatency']))
    STORM_TOPOLOGY_BOLTS_PROCESS_LATENCY.labels(tn,tid,tstatus,bid).set(getMetric(bolt['processLatency']))



def topologyMetric(topology):
    tn = topology['name']
    tid = topology['id']
    tstatus = topology['status']
    for stat in topology['topologyStats']:
        statsMetric(stat,tn,tid,tstatus)
    for spout in topology['spouts']:
        spoutMetric(spout,tn,tid,tstatus)
    for bolt in topology['bolts']:
        boltMetric(bolt,tn,tid,tstatus)



def topologySummaryMetric(topology_summary,stormUiHost):
    tn = topology_summary['name']
    tid = topology_summary['id']
    tstatus = topology_summary['status']
    STORM_TOPOLOGY_UPTIME_SECONDS.labels(tn,tid,tstatus).set(topology_summary['uptimeSeconds'])
    STORM_TOPOLOGY_TASKS_TOTAL.labels(tn,tid,tstatus).set(topology_summary['tasksTotal'])
    STORM_TOPOLOGY_WORKERS_TOTAL.labels(tn,tid,tstatus).set(topology_summary['workersTotal'])
    STORM_TOPOLOGY_EXECUTORS_TOTAL.labels(tn,tid,tstatus).set(topology_summary['executorsTotal'])
    STORM_TOPOLOGY_REPLICATION_COUNT.labels(tn,tid,tstatus).set(topology_summary['replicationCount'])
    STORM_TOPOLOGY_REQUESTED_MEM_ON_HEAP.labels(tn,tid,tstatus).set(topology_summary['requestedMemOnHeap'])
    STORM_TOPOLOGY_REQUESTED_MEM_OFF_HEAP.labels(tn,tid,tstatus).set(topology_summary['requestedMemOffHeap'])
    STORM_TOPOLOGY_REQUESTED_TOTAL_MEM.labels(tn,tid,tstatus).set(topology_summary['requestedTotalMem'])
    STORM_TOPOLOGY_REQUESTED_CPU.labels(tn,tid,tstatus).set(topology_summary['requestedCpu'])
    STORM_TOPOLOGY_ASSIGNED_MEM_ON_HEAP.labels(tn,tid,tstatus).set(topology_summary['assignedMemOnHeap'])
    STORM_TOPOLOGY_ASSIGNED_MEM_OFF_HEAP.labels(tn,tid,tstatus).set(topology_summary['assignedMemOffHeap'])
    STORM_TOPOLOGY_ASSIGNED_TOTAL_MEM.labels(tn,tid,tstatus).set(topology_summary['assignedTotalMem'])
    STORM_TOPOLOGY_ASSIGNED_CPU.labels(tn,tid,tstatus).set(topology_summary['assignedCpu'])

    try:
        r = requests.get('http://'+ stormUiHost +'/api/v1/topology/' + tid)
        topologyMetric(r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        sys.exit(1)


if(len(sys.argv) != 4):
    print('Missing arguments, Usage storm-metrics-consumer.py [StormUI Host] [HTTP port of the consumer] [Refresh Rate in Seconds]')
    sys.exit(-1)

stormUiHost = str(sys.argv[1])
httpPort = int(sys.argv[2])
refreshRate = int(sys.argv[3])

start_http_server(httpPort)

prev_scraped_labels = set()
while True:
    try:
        r = requests.get('http://'+ stormUiHost +'/api/v1/topology/summary')
        print('caught metrics')
        curr_scraped_labels = set()
        for topology in r.json()['topologies']:
            topology_label = topologySummaryMetric(topology,stormUiHost)
            curr_scraped_labels.add(topology_label)
        print('added new metrics')
        for labels in prev_scraped_labels - curr_scraped_labels:
            clearGauges([STORM_TOPOLOGY_UPTIME_SECONDS, STORM_TOPOLOGY_TASKS_TOTAL, STORM_TOPOLOGY_WORKERS_TOTAL, STORM_TOPOLOGY_EXECUTORS_TOTAL,
                         STORM_TOPOLOGY_REPLICATION_COUNT, STORM_TOPOLOGY_REQUESTED_MEM_ON_HEAP, STORM_TOPOLOGY_REQUESTED_MEM_OFF_HEAP,
                         STORM_TOPOLOGY_REQUESTED_TOTAL_MEM, STORM_TOPOLOGY_REQUESTED_CPU, STORM_TOPOLOGY_ASSIGNED_MEM_ON_HEAP,
                         STORM_TOPOLOGY_ASSIGNED_MEM_OFF_HEAP, STORM_TOPOLOGY_ASSIGNED_TOTAL_MEM, STORM_TOPOLOGY_ASSIGNED_CPU, TOPOLOGY_STATS_ACKED,
                         TOPOLOGY_STATS_FAILED, TOPOLOGY_STATS_EMITTED, TOPOLOGY_STATS_TRANSFERRED, TOPOLOGY_STATS_COMPLETE_LATENCY,
                         STORM_TOPOLOGY_SPOUTS_EXECUTORS, STORM_TOPOLOGY_SPOUTS_TASKS, STORM_TOPOLOGY_SPOUTS_ACKED, STORM_TOPOLOGY_SPOUTS_FAILED,
                         STORM_TOPOLOGY_SPOUTS_EMITTED, STORM_TOPOLOGY_SPOUTS_TRANSFERRED, STORM_TOPOLOGY_SPOUTS_LATEST_ERROR_EPOCH,
                         STORM_TOPOLOGY_SPOUTS_COMPLETE_LATENCY, STORM_TOPOLOGY_BOLTS_CAPACITY, STORM_TOPOLOGY_BOLTS_EXECUTORS,STORM_TOPOLOGY_BOLTS_TASKS,
                         STORM_TOPOLOGY_BOLTS_ACKED, STORM_TOPOLOGY_BOLTS_FAILED, STORM_TOPOLOGY_BOLTS_EMITTED, STORM_TOPOLOGY_BOLTS_TRANSFERRED,
                         STORM_TOPOLOGY_BOLTS_LATEST_ERROR_EPOCH, STORM_TOPOLOGY_BOLTS_PROCESS_LATENCY, STORM_TOPOLOGY_BOLTS_EXECUTE_LATENCY],
                        labels)
        print('cleared stale gauges')
        prev_scraped_labels = curr_scraped_labels
        print('processed metrics')
    except requests.exceptions.RequestException as e:
        traceback.print_exc()
        sys.exit(1)
    time.sleep(refreshRate)
