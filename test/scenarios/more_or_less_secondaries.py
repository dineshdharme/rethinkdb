# TODO

import sys, os, time
import lib.workload
from lib.cluster import Cluster
from lib.scenario import Scenario
# import http_admin, driver, workload_runner, scenario_common

class ReplicaSequence(object):
    def __init__(self, steps):
        self.initial = current = steps[0]
        self.totals = []
        self.peak = 0
        for step in self.steps[1:]:
            current += step
            assert current >= 0
            self.totals += [current]
            self.peak = max(current, self.peak)

class MoreOrLessSecondaries(Scenario):
    def __init__(self, sequence, workload):
        Scenario.__init__(self)
        self.sequence = ReplicaSequence(sequence)
        # assert isinstance(workloads, SplitWorkload) TODO
        self.workloads = workloads
    
    def run(self, cluster):
        primary = cluster.new_server(name="primary")
        replicas = [cluster.new_sever(name="replica%n" % i)
                    for i in range(0, self.sequence.peak)]
        primary_dc = cluster.new_datacenter(name="primary", servers=[primary])
        replica_dc = cluster.new_datacenter(name="primary", servers=replicas)        
        # TODO ns = scenario_common.prepare_table_for_workload(opts, http, primary = primary_dc, affinities = {primary_dc: 0, replica_dc: opts["sequence"].initial})
        table = cluster.new_table(primary=primary_dc, affinities={ primary_dc: 0, replica_dc: self.sequence.initial })
        table.wait_until_blueprint_satisfied()
        cluster.check()
        cluster.check_no_issues() # TODO: merge with above
        
        with workload.run_continuous(table):
            workload.run_before(table)
            cluster.check()
            cluster.check_no_issues() # TODO
            workload.check(table)
            for i, n in enumerate(self.sequence.totals):
                if i != 0:
                    workload.run_between(table)
                table.set_namespace_affinities({primary_dc: 0, replica_dc: current})
                table.wait_until_blueprint_satisfied(timeout = 3600)
                cluster.check()
                cluster.check_no_issues()# TODO
                workload.run_after(table)

                cluster.check_no_issues()# TODO
        cluster.check_and_stop()
