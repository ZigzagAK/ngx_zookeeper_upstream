Name
====

ngx_zookeeper_upstream - Upstream synced with Zookeeper.

# Quick Start

## Nginx config

```nginx
http {
  zookeeper_upstream                  127.0.0.1:2181;
  zookeeper_upstream_log_level        debug;
  zookeeper_upstream_recv_timeout     5000;

  healthcheck_buffer_size 128k;

  upstream app1 {
    zone shm_app1 128k;

    zookeeper_sync_path /instances/apps/app1/nodes;
    zookeeper_sync_lock /instances/apps/.locks/app1;

    zookeeper_sync_params @params max_conns=33 max_fails=1 fail_timeout=30s;
    zookeeper_sync_file app1.peers;

    dns_update 60s;
    dns_add_down on;

    check passive type=http rise=2 fall=2 timeout=5000 interval=10;
    check_request_uri GET /health;
    check_response_codes 200;
    check_response_body alive;
  }

  upstream app1-@dc1 {
    zone shm_app1-dc1 128k;

    zookeeper_sync_path /instances/apps/app1/nodes;
    zookeeper_sync_lock /instances/apps/.locks/app1-@dc1;

    zookeeper_sync_filter @dc1;

    zookeeper_sync_params @params max_conns=10 max_fails=1 fail_timeout=30s;
    zookeeper_sync_file app1-@dc1.peers;

    dns_update 60s;
    dns_add_down on;

    check passive type=http rise=2 fall=2 timeout=5000 interval=10;
    check_request_uri GET /health;
    check_response_codes 200;
    check_response_body alive;
  }

  upstream app1-@dc2 {
    zone shm_app1-dc2 128k;

    zookeeper_sync_path /instances/apps/app1/nodes;
    zookeeper_sync_lock /instances/apps/.locks/app1-@dc2;

    zookeeper_sync_filter @dc2;

    zookeeper_sync_params @params max_conns=10 max_fails=1 fail_timeout=30s;
    zookeeper_sync_file app1-@dc2.peers;

    dns_update 60s;
    dns_add_down on;

    check passive type=http rise=2 fall=2 timeout=5000 interval=10;
    check_request_uri GET /health;
    check_response_codes 200;
    check_response_body alive;
  }

  upstream app2 {
    zone shm_app2 128k;

    zookeeper_sync_path /instances/apps/app2/nodes;

    zookeeper_sync_params @params max_conns=20 max_fails=1 fail_timeout=30s;
    zookeeper_sync_file app2.peers;

    dns_update 60s;
    dns_add_down on;

    check passive type=http rise=2 fall=2 timeout=5000 interval=10;
    check_request_uri GET /health;
    check_response_codes 200;
    check_response_body alive;
  }

  server {
    listen 6000;

    location /dynamic {
      dynamic_upstream;
    }
  }

  server {
    # app1
    listen 8001;
    listen 8002;

    #app2
    listen 9001;
    listen 9002;

    location = /health {
      return 200 'alive';
    }

    location / {
      access_log off;
      return 200 'hello';
    }
  }

  server {
    listen 10000;

    access_log off;

    location /app1 {
      proxy_pass http://app1;
    }

    location /app1/dc1 {
      proxy_pass http://app1-@dc1;
    }

    location /app1/dc2 {
      proxy_pass http://app1-@dc2;
    }

    location /app2 {
      proxy_pass http://app2;
    }
  }

  server {
    listen 8888;

    location /dynamic {
      dynamic_upstream;
    }

    location /healthcheck/get {
      healthcheck_get;
    }

    location /healthcheck/status {
      healthcheck_status;
    }
  }
}
````

## Zookeeper structure

### 1
![Zookeeper structure1](zoo1.png)

### 2
![Zookeeper structure2](zoo2.png)

### 3
![Zookeeper structure3](zoo3.png)
