
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
create('/test_upstream/app1/nodes/127.0.1.1:5555');
create('/test_upstream/app1/nodes/127.0.0.1:5555', '@all');
create('/test_upstream/app1/nodes/127.0.0.2:5555', '@a');
create('/test_upstream/app1/nodes/127.0.0.3:5555', '@b');
create('/test_upstream/app2/nodes/127.0.1.4:4444');
create('/test_upstream/app2/nodes/127.0.0.4:4444', '@all');
create('/test_upstream/app2/nodes/127.0.0.5:4444', '@a');
create('/test_upstream/app2/nodes/127.0.0.6:4444', '@b');
create('/test_upstream/.locks');
create('/test_upstream/.locks/app2');
create('/test_upstream/.locks/app2-a');
create('/test_upstream/.locks/app2-b');

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

    upstream app1 {
      zone shm_app1 128k;
      zookeeper_sync_path /test_upstream/app1/nodes;
      zookeeper_sync_file app1.peers;
    }

    upstream app1-a {
      zone shm_app1-a 128k;
      zookeeper_sync_path /test_upstream/app1/nodes;
      zookeeper_sync_file app1.peers;
      zookeeper_sync_filter @a;
    }

    upstream app1-b {
      zone shm_app1-b 128k;
      zookeeper_sync_path /test_upstream/app1/nodes;
      zookeeper_sync_file app1.peers;
      zookeeper_sync_filter @b;
    }

    upstream app2 {
      zone shm_app2 128k;
      zookeeper_sync_path /test_upstream/app2/nodes;
      zookeeper_sync_lock /test_upstream/.locks/app2;
      zookeeper_sync_file app2.peers;
    }

    upstream app2-a {
      zone shm_app2-a 128k;
      zookeeper_sync_path /test_upstream/app2/nodes;
      zookeeper_sync_lock /test_upstream/.locks/app2-a;
      zookeeper_sync_file app2.peers;
      zookeeper_sync_filter @a;
    }

    upstream app2-b {
      zone shm_app2-b 128k;
      zookeeper_sync_path /test_upstream/app2/nodes;
      zookeeper_sync_lock /test_upstream/.locks/app2-b;
      zookeeper_sync_file app2.peers;
      zookeeper_sync_filter @b;
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
qr/\[{"name":"app1","lock":"","params_tag":"\@params","filter":""},\r
{"name":"app1-a","lock":"","params_tag":"\@params","filter":"\@a"},\r
{"name":"app1-b","lock":"","params_tag":"\@params","filter":"\@b"},\r
{"name":"app2","lock":"\/test_upstream\/.locks\/app2\/.+","params_tag":"\@params","filter":""},\r
{"name":"app2-a","lock":"\/test_upstream\/.locks\/app2-a\/.+","params_tag":"\@params","filter":"\@a"},\r
{"name":"app2-b","lock":"\/test_upstream\/.locks\/app2-b\/.+","params_tag":"\@params","filter":"\@b"}\]/


=== STEP 3: Check peers
--- request eval
[
"GET /dynamic?upstream=app1",
"GET /dynamic?upstream=app1-a",
"GET /dynamic?upstream=app1-b",
"GET /dynamic?upstream=app2",
"GET /dynamic?upstream=app2-a",
"GET /dynamic?upstream=app2-b"
]
--- response_body_like eval
[
qr/(server 127\.0\.[01]\.[123]:5555 addr=127\.0\.[01]\.[123]:5555;\n){4}/,
qr/(server 127\.0\.[01]\.[12]:5555 addr=127\.0\.[01]\.[12]:5555;\n){2}/,
qr/(server 127\.0\.[01]\.[13]:5555 addr=127\.0\.[01]\.[13]:5555;\n){2}/,
qr/(server 127\.0\.[01]\.[456]:4444 addr=127\.0\.[01].[456]:4444;\n){4}/,
qr/(server 127\.0\.[01]\.[45]:4444 addr=127\.0\.[01]\.[45]:4444;\n){2}/,
qr/(server 127\.0\.[01]\.[46]:4444 addr=127\.0\.[01]\.[46]:4444;\n){2}/
]
