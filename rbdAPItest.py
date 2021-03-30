import rbd
import rados
import json

# Issues - rbd api does not provide the snapshot schedule interaction
# schedule is implemented by the rbd_support module (always on module)
#   schedules are written to each pool as objects
#   https://github.com/ceph/ceph/blob/d6d88c18c450affa27a73b74fcb4bf122fbec7bf/src/pybind/mgr/rbd_support/schedule.py#L422

# https://github.com/ceph/ceph/blob/master/src/pybind/mgr/rbd_support/schedule.py
# https://github.com/ceph/ceph/blob/master/src/pybind/mgr/rbd_support/mirror_snapshot_schedule.py

with rados.Rados(conffile='/etc/ceph/rh8ceph1.conf') as cluster:
    with cluster.open_ioctx('rbd') as ioctx:
        rbd = rbd.RBD()
        print("\nrbd mirror image status - most of mirror image status pool/imageName cmd")
        for r in rbd.mirror_image_status_list(ioctx):
            print(r)
        #
        # {'name': 'rh8ceph1disk', 'id': '384d894deb8c', 'info': {'global_id': '3b90913f-c87d-4d8b-8c7b-cfcb71d93432', 'state': 1}, 'remote_statuses': ['state', 'description', 'last_update', 'up', 'mirror_uuid'], 'state': 6, 'description': 'local image is primary', 'last_update': datetime.datetime(2021, 3, 30, 3, 33, 56), 'up': True}

        print("\nSummary content displayed in mirror pool status for image states (not health, daemon health, image_health)")
        print(rbd.mirror_image_status_summary(ioctx))
        # [(4, 1)]

        print("\nmirror image info by id")
        for s in rbd.mirror_image_info_list(ioctx):
            print(s)
        # ('384d894deb8c', {'mode': 1, 'global_id': '3b90913f-c87d-4d8b-8c7b-cfcb71d93432', 'state': 1, 'primary': True})

        print("\n peers of a pool")
        for p in rbd.mirror_peer_list(ioctx):
            print(p)
        # {'uuid': 'a478cb24-8747-4085-9bd2-3f11c918e950', 'direction': 2, 'site_name': 'rh8ceph2', 'cluster_name': 'rh8ceph2', 'mirror_uuid': '844be542-2c91-48f6-a3ab-43acd698c62f', 'client_name': 'client.rbd-mirror-peer'}

        print("\npeer environment")
        remote=rbd.mirror_peer_get_attributes(ioctx, p['uuid'])
        print(remote)
        # {'key': 'AQCNzFNg/YD8IxAA6VNxGcDpTgLD0PkfRcPz8A==', 'mon_host': '[v2:192.168.122.119:3300/0,v1:192.168.122.119:6789/0]'}

        print("\nmirror image instance id list")
        for m in rbd.mirror_image_instance_id_list(ioctx):
            print(m)

print("\nschedules")
with rados.Rados(conffile='/etc/ceph/rh8ceph1.conf') as cluster:
    # 2 = pool id of the rbd pool
    with cluster.open_ioctx2(2) as ioctx:
        with rados.ReadOpCtx() as read_op:
            it, ret = ioctx.get_omap_vals(read_op, '', "", 128)
            # this read_op must execute, if not the 'it' object will cause segfaults
            ioctx.operate_read_op(read_op, "rbd_mirror_snapshot_schedule")

            it = list(it)
            for k,v in it:
                v=v.decode()
                print(k)
                print(v)
            # 2//384d894deb8c
                #[
                #    {
                #        "interval": "2m",
                #        "start_time": null
                #    }
                #]


print("\nRemote connection test")

#print(rbd_image_list)
