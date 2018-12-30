Name
====

ngx_zookeeper_upstream - Upstream synced with Zookeeper.

# Quick Start

```nginx
http {
  zookeeper_upstream                  127.0.0.1:2181;
  zookeeper_upstream_log_level        debug;
  zookeeper_upstream_recv_timeout     5000;

  upstream app1 {
    zone shm_app1 128k;

    zookeeper_sync_path /instances/apps/app1/nodes;
    zookeeper_sync_file conf/app1.peers;
    zookeeper_sync_lock /instances/apps/.locks/app1;

    dns_update 10s;
    dns_add_down off;

    include app1.peers;
  }

  upstream app2 {
    zone shm_app2 128k;

    zookeeper_sync_path /instances/apps/app2/nodes;
    zookeeper_sync_file conf/app2.peers;

    dns_update 10s;
    dns_add_down off;

    include app2.peers;
  }
}
````