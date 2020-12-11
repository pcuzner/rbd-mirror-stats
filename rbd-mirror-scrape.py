from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from subprocess import run, CompletedProcess, PIPE
import json
from typing import List, Dict
import time
import shutil
import math


def cli(cmd: str) -> CompletedProcess:
    c = cmd.split(' ')
    return run(c, stdout=PIPE, stderr=PIPE)


class Collector:
    rbd_mirror_states = [
        "unknown",
        "error",
        "syncing",
        "starting_replay",
        "replaying",
        "stopping_replay",
        "stopped",
    ]

    def __init__(self, metrics = None):
        self.metrics = metrics
        self.enabled = True if shutil.which('rbd') else False

    def _fetch_mirror_schedules(self) -> List:
        completion = cli('rbd mirror snapshot schedule ls --recursive --format json')
        assert completion.stdout
        return json.loads(completion.stdout.decode('utf-8'))


    def _fetch_pool_status(self, pool: str) -> Dict[str, str]:
        completion = cli(f'rbd -p {pool} mirror pool status --format json')
        assert completion.stdout
        return json.loads(completion.stdout.decode('utf-8'))


    def _fetch_pool_info(self, pool: str) -> Dict[str, str]:
        completion = cli(f'rbd -p {pool} mirror pool info --format json')
        assert completion.stdout
        return json.loads(completion.stdout.decode('utf-8'))

    def dump(self):
        s = ""
        for k in self.metrics:
            if self.metrics[k].value:
                s += str(self.metrics[k])
                self.metrics[k].clear()
        return s

    def collect(self):
        gather_start = time.time()
        if self.enabled:
            schedules = self._fetch_mirror_schedules()
            pools = {}

            for pool_data in schedules:
                pool_name = pool_data['pool']
                if pool_name in pools:
                    pools[pool_name] += 1
                else:
                    pools[pool_name] = 1

            for pool in pools.keys():
                self.metrics['rbd_mirror_snapshot_schedules'].set(pools[pool], (pool,))
                _start = time.time()
                pool_status = self._fetch_pool_status(pool)
                gather_pool_status_time = time.time() - _start
                if all([pool_status['summary'][health].upper() == "OK" for health in ['health', 'daemon_health', 'image_health'] ]):
                    v = 0
                else:
                    v = 1
                self.metrics['rbd_mirror_snapshot_health'].set(v, (
                    pool,
                    pool_status['summary']['health'],
                    pool_status['summary']['daemon_health'],
                    pool_status['summary']['image_health'],
                ))
                ('pool_name', 'site_name', 'peer_site', 'health', 'daemon_health', 'image_health'),
                _start = time.time()
                pool_info = self._fetch_pool_info(pool)
                gather_pool_info_time = time.time() - _start

                peers = pool_info.get('peers', [])
                if peers:
                    self.metrics['rbd_mirror_snapshot_metadata'].set(1, (
                        pool,
                        pool_info.get('mode'),
                        pool_info.get('site_name'),
                        peers[0].get('site_name'),
                        peers[0].get('uuid'),
                        peers[0].get('mirror_uuid'),
                        peers[0].get('direction')
                    ))

                image_states = pool_status['summary'].get('states', {})
                for state in Collector.rbd_mirror_states:
                    v = image_states.get(state, 0)
                    self.metrics['rbd_mirror_snapshot_image_state'].set(v, (pool, state))
                
        gather_end = time.time()  - gather_start
        self.metrics['rbd_mirror_snapshot_scrape_seconds'].set(gather_end)


class Metric:
    def __init__(self, name, description, label_names):
        self._name = name
        self._description = description
        self._label_names = label_names
        self.value = {}

    def __str__(self):

        def floatstr(value):
            if value == float('inf'):
                return '+Inf'
            if value == float('-inf'):
                return '-Inf'
            if math.isnan(value):
                return 'NaN'
            return repr(float(value))

        if not self.value:
            return ""

        s = f"""# HELP {self._name} {self._description}
# TYPE {self._name} gauge
"""

        for item in self.value:
            ptr = 0
            labels = ''
            if self._label_names:
                for l in self._label_names:
                    labels += f'{l}="{item[ptr]}",'
                    ptr += 1

                labels = f"{{{labels[:-1]}}}"
            v = floatstr(self.value[item])
            s += f"""{self._name}{labels} {v}\n"""
        return s
    
    def set(self, value, labelvalues=None):
        labelvalues = labelvalues or ("",)
        self.value[labelvalues] = value

    def clear(self):
        self.value = {}


class MetricsHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.server.collector.collect()
            self.wfile.write(self.server.collector.dump().encode('utf-8'))

        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class Exporter:

    def __init__(self, port=9289):
        self.port = port
        self.metrics = self.init_metrics()
    
    def init_metrics(self):
        metrics = {}
        metrics['rbd_mirror_snapshot_metadata'] = Metric(
            "rbd_mirror_snapshot_metadata",
            "metadata relating to rbd-mirror relationship",
            ('pool_name', 'mode', 'site_name', 'peer_site', 'uuid', 'mirror_uuid', 'direction'),
        )
        metrics['rbd_mirror_snapshot_health'] = Metric(
            "rbd_mirror_snapshot_health",
            "health information for image and local/remote daemons",
            ('pool_name', 'health', 'daemon_health', 'image_health'),
        )
        metrics['rbd_mirror_snapshot_image_state'] = Metric(
            "rbd_mirror_snapshot_image_state",
            "count of images in a given rbd-mirror state",
            ('pool_name', 'status'),
        )
        metrics['rbd_mirror_snapshot_schedules'] = Metric(
            "rbd_mirror_snapshot_schedules",
            "count of snapshot schedules defined against a pool",
            ('pool_name',),
        )
        metrics['rbd_mirror_snapshot_scrape_seconds'] = Metric(
            "rbd_mirror_snapshot_scrape_seconds",
            "temporary metric for POC testing",
            (),
        )

        return metrics

    def run(self):
        httpd = ThreadedHTTPServer(("0.0.0.0", self.port), MetricsHandler)
        httpd.collector = Collector(self.metrics)
        print("collector enabled state is: ", httpd.collector.enabled)

        httpd.serve_forever()


def main():

    exporter = Exporter()
    exporter.init_metrics()
    exporter.run()

if __name__ == '__main__':
    main()