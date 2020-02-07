use Test::Nginx::Socket 'no_plan';
use Net::ZooKeeper qw(:node_flags :acls);

$ENV{TEST_NGINX_ZOOKEEPER_PORT} ||= 2181;

# prepare zoo data

my $zkh = Net::ZooKeeper->new('127.0.0.1:' . $ENV{TEST_NGINX_ZOOKEEPER_PORT});

sub create {
    my ($path, $val) = @_;
    $zkh->create($path, $val || '', 'acl' => ZOO_OPEN_ACL_UNSAFE) or
        die("unable to create node " . $path . ", err:" . $zkh->get_error());
}

sub cleanup {
    my ($path) = @_;
    foreach my $znode ($zkh->get_children($path)) {
      cleanup($path . '/' . $znode);
    }
    $zkh->delete($path);
}

cleanup('/test_upstream');

create('/test_upstream');
create('/test_upstream/app1');
create('/test_upstream/app1/nodes');
create('/test_upstream/app2');
create('/test_upstream/app2/nodes');
create('/test_upstream/app1/nodes/127.0.0.1:5555');
create('/test_upstream/app1/nodes/127.0.0.2:5555');
create('/test_upstream/app1/nodes/global-exclude-host1.com');
create('/test_upstream/app1/nodes/1.2.3.4');
create('/test_upstream/app2/nodes/127.0.0.3:4444');
create('/test_upstream/app2/nodes/localhost:4444');
create('/test_upstream/app2/nodes/global-exclude-host2.com');
create('/test_upstream/app2/nodes/1.2.3.4');
create('/test_upstream/.locks');
create('/test_upstream/.locks/app2');

add_cleanup_handler(sub {
    cleanup('/test_upstream');
});

no_shuffle();
run_tests();

__DATA__

=== STEP 1: Init
--- http_config

    zookeeper_upstream                  127.0.0.1:$TEST_NGINX_ZOOKEEPER_PORT;
    zookeeper_upstream_log_level        debug;
    zookeeper_upstream_recv_timeout     5000;

    zookeeper_sync_global_exclude global-exclude-host1.com;
    zookeeper_sync_global_exclude global-exclude-host2.com;
    zookeeper_sync_global_exclude 1.2.3.4;

    upstream app1 {
      zone shm_app1 128k;
      zookeeper_sync_path /test_upstream/app1/nodes;
      zookeeper_sync_file app1.peers;
      zookeeper_sync_exclude 127.0.0.2;
    }

    upstream app2 {
      zone shm_app2 128k;
      zookeeper_sync_path /test_upstream/app2/nodes;
      zookeeper_sync_lock /test_upstream/.locks/app2;
      zookeeper_sync_file app2.peers;
      zookeeper_sync_exclude localhost;
    }

--- config

    location = /test {
      return 200;
    }

    location = /list {
      zookeeper_sync_list;
    }

    location = /dynamic {
      dynamic_upstream;
    }

--- request
    GET /test
--- wait: 3


=== STEP 2: Check list
--- request
    GET /list
--- response_body_like eval
qr/\[\{"name":"app1","lock":"","params_tag":"\@params","filter":""\},\r
\{"name":"app2","lock":"\/test_upstream\/.locks\/app2\/.+","params_tag":"\@params","filter":""\}\]/


=== STEP 3: Check peers
--- request eval
["GET /dynamic?upstream=app1","GET /dynamic?upstream=app2"]
--- response_body eval
["server 127.0.0.1:5555 addr=127.0.0.1:5555;
",
"server 127.0.0.3:4444 addr=127.0.0.3:4444;
"]


